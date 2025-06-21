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
import gleam/option
import gleam/otp/actor
import glisten
import mug
import resp.{from_bit_array}

pub fn main() {
  let cli_args = argv.load().arguments
  let assert Ok(config) = clad.decode(cli_args, config_decoder())
  let db = database.start(config)

  replica_handshake(config)
  let assert Ok(_) =
    glisten.handler(
      fn(_conn) {
        let subject = process.new_subject()
        database.register(db, subject)
        let selector =
          process.new_selector() |> process.selecting(subject, fn(x) { x })
        #(db, option.Some(selector))
      },
      handle_message,
    )
    |> glisten.serve(config.port)

  process.sleep_forever()
}

fn config_decoder() {
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
  let config =
    database.Config(dir:, db_filename:, port:, replicaof: option.None)
  case replicaof {
    option.Some(replicaof) ->
      case string.split(replicaof, on: " ") {
        [master_host, master_port_str] ->
          case int.parse(master_port_str) {
            Ok(master_port) -> {
              let replicaof = option.Some(#(master_host, master_port))
              let config = database.Config(..config, replicaof:)
              decode.success(config)
            }
            Error(_) -> decode.failure(config, "replicaof")
          }
        _ -> decode.failure(config, "replicaof")
      }
    option.None -> decode.success(config)
  }
}

fn handle_message(msg, state, conn) {
  io.println("Received message!")
  let response = case msg {
    glisten.Packet(msg) -> {
      io.println("Received packet")
      echo msg
      let input = from_bit_array(msg)
      let db = state
      input
      |> result.map(command.parse)
      |> result.map_error(resp.SimpleError)
      |> result.flatten()
      |> result.map(database.handle_command(db, _))
      |> result.unwrap_both()
      |> resp.to_bit_array()
    }
    glisten.User(stream) -> {
      io.println("Received stream")
      stream
    }
  }
  echo response
  let assert Ok(_) = glisten.send(conn, response |> bytes_tree.from_bit_array)
  actor.continue(state)
}

fn replica_handshake(config: database.Config) -> Nil {
  case config.replicaof {
    option.Some(#(master_host, master_port)) -> {
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
    option.None -> Nil
  }
}
