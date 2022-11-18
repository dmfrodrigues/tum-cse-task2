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
  cloud::CloudMessage response;
  response.set_type(msg.type());
  response.set_operation(msg.operation());

  bool success = true;
  auto kvps = msg.kvp();
  for(const auto &kvp: kvps){
    const std::string &key = kvp.key();
    const std::string &value = kvp.value();
    const std::optional<SocketAddress> &addr = routing.find_peer(key);
    if(addr){
      Connection peerCon(*addr);
      
      cloud::CloudMessage peerMessage;
      peerMessage.set_type(msg.type());
      peerMessage.set_operation(msg.operation());
      auto *tmp = peerMessage.add_kvp();
      tmp->set_key(key);
      tmp->set_value(value);

      peerCon.send(peerMessage);

      cloud::CloudMessage peerResponse;
      peerCon.receive(peerResponse);

      for(const auto &p: peerResponse.kvp()){
        auto *tmp = response.add_kvp();
        tmp->set_key(p.key());
        tmp->set_value(p.value());
      }

      success &= peerResponse.success();
    }
  }

  response.set_success(success);
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
  nodes.insert(peer);
  redistribute_partitions();
}

auto RouterHandler::redistribute_partitions() -> void {
  auto peers = routing.partitions_by_peer();
  const size_t &numberPeers = nodes.size();
  const size_t numberPartitionsPerNode = routing.get_partitions()/numberPeers;

  if(peers.empty()){
    uint32_t partition = 0;
    for(const SocketAddress &peer: nodes){
      while(peers[peer].size() < numberPartitionsPerNode){
        routing.add_peer(partition, peer);
        ++partition;
      }
    }
  } else {
    size_t numberPartitionsPerNode = routing.get_partitions()/numberPeers;
    std::set<uint32_t> partitionsReassigned;

    for(auto &p: peers){
      const SocketAddress &peer = p.first;
      auto &partitions = p.second;
      while(partitions.size() > numberPartitionsPerNode){
        uint32_t partition = *partitions.begin();
        partitionsReassigned.insert(partition);
        routing.remove_peer(partition, peer);
        partitions.erase(partition);
      }
    }

    for(auto &p: peers){
      const SocketAddress &peer = p.first;
      auto &partitions = p.second;
      while(!partitionsReassigned.empty() && partitions.size() < numberPartitionsPerNode){
        uint32_t partition = *partitionsReassigned.begin();
        partitionsReassigned.erase(partition);
        routing.add_peer(partition, peer);
        partitions.insert(partition);
      }
    }
  }
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