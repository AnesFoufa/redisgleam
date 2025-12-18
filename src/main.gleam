import argv
import clad
import command
import database
import gleam/bytes_tree
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/string
import glisten
import replication
import resp

pub fn main() {
  let cli_args = argv.load().arguments
  let assert Ok(config) = clad.decode(cli_args, config_decoder())
  let db = database.start(config)

  replication.perform_handshake(config, db)
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

fn handle_message(msg, db, conn) {
  io.println("Received message!")
  let response = case msg {
    glisten.Packet(msg) -> {
      io.println("Received packet")
      resp.from_bit_array(msg)
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
  let assert Ok(_) = glisten.send(conn, response |> bytes_tree.from_bit_array)
  actor.continue(db)
}
