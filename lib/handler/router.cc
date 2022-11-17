#include "cloudlab/handler/router.hh"

#include "fmt/core.h"

#include "cloud.pb.h"

namespace cloudlab {

auto RouterHandler::handle_connection(Connection& con) -> void {
  cloud::CloudMessage request{}, response{};

  if (!con.receive(request)) {
    return;
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(request.operation());

  switch (request.operation()) {
    case cloud::CloudMessage_Operation_PUT:
    case cloud::CloudMessage_Operation_GET:
    case cloud::CloudMessage_Operation_DELETE: {
      handle_key_operation(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_JOIN_CLUSTER: {
      handle_join_cluster(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_PARTITIONS_ADDED: {
      handle_partitions_added(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_PARTITIONS_REMOVED: {
      handle_partitions_removed(con, request);
      break;
    }
    default:
      break;
  }
}

auto RouterHandler::handle_key_operation(Connection& con,
                                         const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

auto RouterHandler::handle_join_cluster(Connection& con,
                                        const cloud::CloudMessage& msg)
    -> void {
  add_new_node(SocketAddress(msg.address().address()));

  cloud::CloudMessage response{};
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);
  response.set_success(true);

  con.send(response);
}

auto RouterHandler::add_new_node(const SocketAddress& peer) -> void {
  auto peers = routing.partitions_by_peer();
  size_t numberPeers = peers.size()+1;

  if(peers.empty()){
    for(uint32_t partition = 0; partition < routing.get_partitions(); ++partition){
      routing.add_peer(partition, peer);
    }
  } else {
    size_t numberPartitionsPerNode = routing.get_partitions()/numberPeers;

    for(auto &p: peers){
      const SocketAddress &oldPeer = p.first;
      auto &partitions = p.second;
      while(partitions.size() > numberPartitionsPerNode){
        uint32_t partition = *partitions.begin();
        routing.remove_peer(partition, oldPeer);
        routing.add_peer(partition, peer);
        partitions.erase(partition);
      }
    }
  }
}

auto RouterHandler::redistribute_partitions() -> void {
  // TODO (you)
}

auto RouterHandler::handle_partitions_added(Connection& con,
                                            const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

auto RouterHandler::handle_partitions_removed(Connection& con,
                                              const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

}  // namespace cloudlab