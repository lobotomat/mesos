#include <string>
#include <sstream>
#include <vector>

#include <stout/check.hpp>
#include <stout/error.hpp>
#include <stout/os.hpp>

#include <libtorrent/bencode.hpp>
#include <libtorrent/entry.hpp>
#include <libtorrent/magnet_uri.hpp>

#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/http.hpp>
#include <process/process.hpp>
#include <process/shared.hpp>

#include <stout/path.hpp>
#include <stout/strings.hpp>

#include "peer.hpp"


using libtorrent::torrent_handle;
using libtorrent::torrent_status;
using libtorrent::error_code;

using process::PID;
using process::Future;
using process::Owned;
using process::Promise;

using process::http::decode;
using process::http::BadRequest;
using process::http::InternalServerError;
using process::http::Request;
using process::http::Response;
using process::http::OK;

using std::string;
using std::stringstream;
using std::endl;
using std::vector;

void PeerProcess::initialize()
{
  // Register for a debug routing.
  route("/hello", None(), [=] (const process::http::Request& request) {
      string body = "hello world!";
      return process::http::OK(body);
    });

  // Setup "add" torrent route.
  // NOTE: The magnet link has to be double URL encoded. A common magnet
  // link is URL-encoded already but since we are using it as a get-parameter
  // instead of a full URI, it has to be URL encoded again.
  // Sample request:
  // http://127.0.0.1:52761/(1)/add?magnet%3A%3Fdn%3Daqos-aoc.2013.brrip.xvid.avi%26xt%3Durn%253Abtih%253ASILPEVA7KCCFDWYIYFMDQBTHDH6IYLUJ%26tr%3D192.168.178.20%253A6969
  route ("/add", None(), [=] (const Request& request) -> Future<Response> {
      LOG(INFO) << "add requested";

      // Extract and double url-decode the magnet link.
      size_t found = request.url.find("/add");
      CHECK(found != string::npos);
      string link = request.url.substr(found + 5);

      Try<string> decoded = decode(link);
      if (decoded.isError()) {
        LOG(ERROR) << "failed to decode link with " << decoded.error();
        return BadRequest(decoded.error());
      }
      decoded = decode(decoded.get());
      if (decoded.isError()) {
        LOG(ERROR) << "failed to decode link with " << decoded.error();
        return BadRequest(decoded.error());
      }

      LOG(INFO) << "link: " << decoded.get();

      error_code ec;
      libtorrent::add_torrent_params p;

      libtorrent::parse_magnet_uri(decoded.get(), p, ec);
      if (ec)
      {
        LOG(ERROR) << "failed to parse magnet link: " << ec.message();
        return BadRequest(ec.message());
      }

      p.save_path = "./";

      torrent_handle torrent = session.add_torrent(p, ec);
      if (ec)
      {
        LOG(ERROR) << "failed to add torrent: " << ec.message();
        return InternalServerError(ec.message());
      }

      Owned<Promise<Response>> download(new Promise<Response>);

      process::delay(
          Seconds(1),
          self(),
          &PeerProcess::update,
          torrent,
          download);

      return download->future();
    });

  // Setup "stats" overview route.
  route("/stats", None(), [=] (const process::http::Request& request) {
      LOG(INFO) << "stats requested";

      // Render session status.
      std::stringstream out;
      out << "listening: " << session.is_listening() << endl;
      out << "dht active: " << session.is_dht_running() << endl;

      // Render sessioned torrent details.
      unsigned int i = 0;
      const vector<torrent_handle>& torrents(session.get_torrents());

      foreach(torrent_handle torrent, torrents) {
        out << "torrent(" << i << "): " << torrent.name() << endl;

        const torrent_status& status(torrent.status());
        out << "\tadded_time: " << status.added_time << endl;
        out << "\tis finished: " << status.is_finished << endl;
        out << "\tis seeding: " << status.is_seeding << endl;
        out << "\tnumber of seeds: " << status.num_seeds << endl;
        out << "\tnumber of peers: " << status.num_peers << endl;
        out << "\tdownloaded: " << status.all_time_download << endl;
        out << "\tuploaded: " << status.all_time_upload << endl;
        out << "\tdownload rate: " << status.download_payload_rate << endl;
        out << "\tupload rate: " << status.upload_payload_rate << endl;

        i++;
      }

      return OK(out.str());
    });

  // Listen to the stop event.
  install("stop", [=] (const process::UPID& from, const std::string& body) {
      terminate(self());
    });

  // Initiate torrent session.
  libtorrent::error_code ec;
  session.listen_on(std::make_pair(6881, 6889), ec);
  if (ec)
  {
    LOG(FATAL) << "failed to open listen socket: " << ec.message();
  }
  session.start_dht();
}


void PeerProcess::update(
    torrent_handle torrent,
    Owned<Promise<Response>> download)
{
  const torrent_status& status(torrent.status());

  LOG(INFO) << "downloaded: " << status.all_time_download;
  
  // Are we done yet?
  if (status.is_finished) {
    download->set(OK("done"));
  }
  
  // Retry after a second of idleness.
  process::delay(
      Seconds(1),
      self(),
      &PeerProcess::update,
      torrent,
      download);
}


int main(int argc, char* argv[])
{
  PeerProcess process;
  PID<PeerProcess> pid = spawn(&process);

  LOG(INFO) << "id: " << process.self().id;
  LOG(INFO) << "port: " << process.self().port;

  wait(pid);

  return 0;
}
