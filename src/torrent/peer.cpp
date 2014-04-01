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


  route ("/tor", None(), [=] (const Request& request) -> Future<Response> {
      LOG(INFO) << "add torrent file requested";

      // Extract the magnet link.
      Option<string> file = request.query.get("file");
      if (file.isNone()) {
        LOG(ERROR) << "file missing in " << request.url;
        return BadRequest("file missing");
      }

      LOG(INFO) << "file: " << file.get();

      // Extract the destination directory.
      Option<string> directory = request.query.get("directory");
      if (directory.isNone()) {
        LOG(ERROR) << "directory missing in " << request.url;
        return BadRequest("directory missing");
      }

      error_code ec;
      libtorrent::add_torrent_params p;

      // Add the given destination directory as a save path to the torrent.
      p.save_path = directory.get();

      //
      p.ti = new libtorrent::torrent_info(file.get(), ec);
      if (ec)
      {
        LOG(ERROR) << "failed to add torrent: " << ec.message();
        return InternalServerError(ec.message());
      }

      // Add the torrent to our session.
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


  // Setup "mag" torrent route. This function does return once the download
  // of the given torrent is in a final state.
  //
  // NOTE: The magnet link has to be double URL encoded. A common magnet
  // link is partially URL-encoded already but since we are using it as a
  // get-parameter instead of a full URI, it has to be URL encoded again.
  //
  // Sample request:
  // http://127.0.0.1:56020/(1)/mag?uri=magnet%253A%253Fdn%253Dmesos_0.18.0-rc4_amd64.deb%2526xt%253Durn%25253Abtih%25253APNXCLLNKG7LAFUSXLNT6WE5OSYFD3EVF&directory=.%2F&size=44436622&web=http%3A%2F%2Fdownloads.mesosphere.io%2Fmaster%2Fubuntu%2F13.10%2Fmesos_0.18.0-rc4_amd64.deb
  route ("/mag", None(), [=] (const Request& request) -> Future<Response> {
      LOG(INFO) << "add magnet link requested";

      LOG(INFO) << "request: " << stringify(request.query);

      // Extract the magnet link.
      Option<string> link = request.query.get("uri");
      if (link.isNone()) {
        LOG(ERROR) << "uri missing in " << request.url;
        return BadRequest("uri missing");
      }

      // Double URL decode the link.
      Try<string> decoded = decode(link.get());
      if (decoded.isError()) {
        LOG(ERROR) << "failed to decode uri with " << decoded.error();
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

      // Parse the given magnet link.
      libtorrent::parse_magnet_uri(decoded.get(), p, ec);
      if (ec)
      {
        LOG(ERROR) << "failed to parse magnet link: " << ec.message();
        return BadRequest(ec.message());
      }

      // Extract the destination directory.
      Option<string> directory = request.query.get("directory");
      if (directory.isNone()) {
        LOG(ERROR) << "directory missing in " << request.url;
        return BadRequest("directory missing");
      }

      LOG(INFO) << "directory: " << directory.get();

      // Add the given destination directory as a save path to the torrent.
      p.save_path = directory.get();

      // Add the torrent to our session.
      torrent_handle torrent = session.add_torrent(p, ec);
      if (ec)
      {
        LOG(ERROR) << "failed to add torrent: " << ec.message();
        return InternalServerError(ec.message());
      }

      // Add an optional webseed to the torrent.
      // NOTE: This is done manually as libtorrent does not support
      // extracting "Alternative Sources" or "eXact Sources" from magnet
      // links.
      Option<string> webSeed = request.query.get("web");
      if (webSeed.isSome()) {
        torrent.add_http_seed(webSeed.get());
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

        unsigned int s = 0;
        const std::set<string>& seeds = torrent.http_seeds();
        foreach(string seed, seeds) {
          out << "\tweb seed(" << s << "): " << seed << endl;
          s++;
        }

        if (torrent.is_valid()) {
          const torrent_status& status(torrent.status());
          out << "\ttotal size: " << status.total_wanted << endl;
          out << "\tadded_time: " << status.added_time << endl;
          out << "\tis finished: " << status.is_finished << endl;
          out << "\tis seeding: " << status.is_seeding << endl;
          out << "\tnumber of seeds: " << status.num_seeds << endl;
          out << "\tnumber of peers: " << status.num_peers << endl;
          out << "\thas incoming: " << status.has_incoming << endl;
          out << "\tdownloaded: " << status.all_time_download << endl;
          out << "\tuploaded: " << status.all_time_upload << endl;
          out << "\tdownload rate: " << status.download_payload_rate << endl;
          out << "\tupload rate: " << status.upload_payload_rate << endl;
        }
        i++;
      }

      return OK(out.str());
    });

  // Listen to the stop event.
  install("stop", [=] (const process::UPID& from, const std::string& body) {
      terminate(self());
    });

  // Initiate torrent session.
  error_code ec;
  session.listen_on(std::make_pair(6881, 6889), ec);
  if (ec)
  {
    LOG(FATAL) << "failed to open listen socket: " << ec.message();
  }

  // Enable DHT.
  session.start_dht();
}


void PeerProcess::update(
    torrent_handle torrent,
    Owned<Promise<Response>> download)
{
  const torrent_status& status(torrent.status());

  LOG(INFO) << "metadata: " << status.has_metadata << ", "
            << "downloaded: " << status.all_time_download;

  // When the download is finished, satisfy the promise.
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

  // TODO(tillt): Persist this information in a known location to make it
  // available to clients.
  LOG(INFO) << "id: " << process.self().id;
  LOG(INFO) << "port: " << process.self().port;

  wait(pid);

  return 0;
}
