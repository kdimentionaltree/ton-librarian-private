/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#include <atomic>
#include <cstdio>
#include <string>

#include "Config.h"
#include "LibrarianNode.h"
#include "td/actor/actor.h"
#include "vm/vm.h"

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: ton-librarian <config.json>\n");
    return 2;
  }
  std::string config_path = argv[1];

  auto config_r = tonlibrarian::Config::parse_file(config_path);
  if (config_r.is_error()) {
    std::fprintf(stderr, "failed to parse config %s: %s\n", config_path.c_str(),
                 config_r.move_as_error().to_string().c_str());
    return 1;
  }
  auto config = config_r.move_as_ok();
  auto st = config.validate();
  if (st.is_error()) {
    std::fprintf(stderr, "invalid config: %s\n", st.to_string().c_str());
    return 1;
  }

  vm::init_vm().ensure();

  // Scheduler layout: node 0 hosts the coordinator and the network stack;
  // nodes 1..N host one ParseWorker each (see LibrarianNode::init_parse_workers).
  td::uint32 parse_workers = std::max<td::uint32>(1, config.pipeline.parse_workers);
  std::vector<td::actor::Scheduler::NodeInfo> nodes;
  nodes.emplace_back(3);  // coordinator + network actors
  for (td::uint32 i = 0; i < parse_workers; i++) {
    nodes.emplace_back(1);
  }

  std::atomic<bool> done{false};
  // hold the actor (validator-engine pattern): a discarded ActorOwn would
  // immediately destroy the node
  td::actor::ActorOwn<tonlibrarian::LibrarianNode> node;
  td::actor::Scheduler scheduler(nodes);
  scheduler.run_in_context([&] {
    node = tonlibrarian::LibrarianNode::create(std::move(config), &done);
  });
  while (scheduler.run(1)) {
    if (done.load()) {
      scheduler.stop();
    }
  }
  std::fprintf(stderr, "ton-librarian finished\n");
  return 0;
}
