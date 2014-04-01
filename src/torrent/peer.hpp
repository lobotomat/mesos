#ifndef __TORRENT_PEER_HPP__
#define __TORRENT_PEER_HPP__

#include <libtorrent/session.hpp>

#include <process/future.hpp>
#include <process/process.hpp>
#include <process/shared.hpp>

// Implements a libtorrent peer, listening to HTTP requests for control.
// The interface strictly follows the libprocess::process implementation.
//
// The following control events are implemented:
// stats  - Displays status information for the bittorrent session.
// mag    - Downloads a file and keeps seeding it when done.
//
// The following control events may be implemented in the future:
// tor    - Downloads a file and keeps seeding it when done.
//
class PeerProcess : public process::Process<PeerProcess>
{
public:
  PeerProcess() {}
  virtual ~PeerProcess() {}

protected:
  // Setup route and event handlers, start the bittorrent session.
  virtual void initialize();

  // Re/check the download status of a specific torrent.
  void update(
      libtorrent::torrent_handle torrent,
      process::Owned<process::Promise<process::http::Response>> download);

private:
  libtorrent::session session;
};

#endif // __TORRENT_PEER_HPP__
