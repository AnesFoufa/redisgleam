import argv
import clad
import command
import database
import gleam/bit_array
import gleam/bytes_tree
import gleam/int
import gleam/io
import gleam/result
import gleam/string

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten
import mug
import resp.{from_bit_array}

pub fn main() {
  let config_decoder = {
    use dir <- decode.optional_field("dir", "/var/lib/redis", decode.string)
    use db_filename <- decode.optional_field(
      "dbfilename",
      "dump.rdb",
      decode.string,
    )
    use port <- decode.optional_field("port", 6379, decode.int)
    use replicaof <- decode.optional_field(
      "replicaof",
      option.None,
      decode.map(decode.string, option.Some),
    )
    decode.success(database.Config(dir:, db_filename:, port:, replicaof:))
  }
  let assert Ok(config) = clad.decode(argv.load().arguments, config_decoder)
  let db = database.start(config)

  // Handshake: if replica configuration is provided, connect to master and send PING command.
  case config.replicaof {
    option.Some(replica_info) -> replica_handshake(replica_info, config)
    option.None -> io.println("No replica configuration provided")
  }
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      io.println("Received message!")
      let assert glisten.Packet(msg) = msg
      let input = from_bit_array(msg)
      let response =
        input
        |> result.map(command.parse)
        |> result.map_error(resp.SimpleError)
        |> result.flatten()
        |> result.map(command.handle(config, db, _))
        |> result.unwrap_both()
        |> resp.to_bit_array()
        |> bytes_tree.from_bit_array()

      let assert Ok(_) = glisten.send(conn, response)
      actor.continue(state)
    })
    |> glisten.serve(config.port)

  process.sleep_forever()
}

fn replica_handshake(replica_info: String, config: database.Config) -> Nil {
  let parts = string.split(replica_info, " ")
  case parts {
    [master_host, master_port_str] ->
      case int.parse(master_port_str) {
        Ok(master_port) -> {
          io.println(
            "Connecting to master "
            <> master_host
            <> ":"
            <> int.to_string(master_port),
          )
          let assert Ok(socket) =
            mug.new(master_host, port: master_port)
            |> mug.timeout(milliseconds: 500)
            |> mug.connect()
          let ping_cmd =
            resp.Array([resp.BulkString(bit_array.from_string("PING"))])
          let ping_bs = resp.to_bit_array(ping_cmd)
          let assert Ok(Nil) = mug.send(socket, ping_bs)
          let assert Ok(_pong_packet) =
            mug.receive(socket, timeout_milliseconds: 100)

          // Send REPLCONF listening-port <PORT> command
          let listening_port_cmd =
            resp.Array([
              resp.BulkString(bit_array.from_string("REPLCONF")),
              resp.BulkString(bit_array.from_string("listening-port")),
              resp.BulkString(bit_array.from_string(int.to_string(config.port))),
            ])
          let listening_port_bs = resp.to_bit_array(listening_port_cmd)
          let assert Ok(Nil) = mug.send(socket, listening_port_bs)
          let assert Ok(_repl_conf_listening_port_packet) =
            mug.receive(socket, timeout_milliseconds: 100)
          // Send REPLCONF capa psync2 command
          let capa_cmd =
            resp.Array([
              resp.BulkString(bit_array.from_string("REPLCONF")),
              resp.BulkString(bit_array.from_string("capa")),
              resp.BulkString(bit_array.from_string("psync2")),
            ])
          let capa_bs = resp.to_bit_array(capa_cmd)
          let assert Ok(Nil) = mug.send(socket, capa_bs)
          let assert Ok(_repl_conf_capa_packet) =
            mug.receive(socket, timeout_milliseconds: 100)
          // Send PSYNC command
          let psync_cmd =
            resp.Array([
              resp.BulkString(bit_array.from_string("PSYNC")),
              resp.BulkString(bit_array.from_string("?")),
              resp.BulkString(bit_array.from_string("-1")),
            ])
          let psync_bs = resp.to_bit_array(psync_cmd)
          let assert Ok(Nil) = mug.send(socket, psync_bs)
          let assert Ok(_psync_packet) =
            mug.receive(socket, timeout_milliseconds: 100)
          Nil
        }
        Error(_) -> io.println("Invalid master port")
      }
    _ -> io.println("Invalid replicaof configuration format")
  }
}
