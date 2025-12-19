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
        let selector =
          process.new_selector() |> process.selecting(subject, fn(x) { x })
        #(subject, option.Some(selector))
      },
      make_handler(db),
    )
    |> glisten.serve(config.port)

  process.sleep_forever()
}

fn make_handler(db: database.Database) {
  fn(msg, subject, conn) {
    io.println("Received message!")

    let updated_subject = case msg {
      glisten.Packet(data) -> {
        io.println("Received packet")
        handle_packet(data, db, subject, conn)
      }
      glisten.User(stream) -> {
        io.println("Received stream")
        handle_user_message(stream, subject, conn)
      }
    }
    actor.continue(updated_subject)
  }
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

fn handle_packet(
  data: BitArray,
  db: database.Database,
  subject: process.Subject(BitArray),
  conn,
) -> process.Subject(BitArray) {
  case parse_command(data) {
    Ok(cmd) -> {
      execute_and_respond(cmd, db, subject, conn)
      subject
    }
    Error(error) -> {
      send_error(error, conn)
      subject
    }
  }
}

fn parse_command(data: BitArray) -> Result(command.Command, resp.Resp) {
  resp.from_bit_array(data)
  |> result.map(command.parse)
  |> result.map_error(resp.SimpleError)
  |> result.flatten()
}

fn execute_and_respond(
  cmd: command.Command,
  db: database.Database,
  subject: process.Subject(BitArray),
  conn,
) -> Nil {
  case database.handle_command(db, cmd, subject) {
    database.SendResponse(response) -> send_response(response, conn)
    database.Silent -> Nil
  }
}

fn send_response(response: resp.Resp, conn) -> Nil {
  let data = resp.to_bit_array(response)
  let assert Ok(_) = glisten.send(conn, data |> bytes_tree.from_bit_array)
  Nil
}

fn send_error(error: resp.Resp, conn) -> Nil {
  let data = resp.to_bit_array(error)
  let assert Ok(_) = glisten.send(conn, data |> bytes_tree.from_bit_array)
  Nil
}

fn handle_user_message(
  stream: BitArray,
  subject: process.Subject(BitArray),
  conn,
) -> process.Subject(BitArray) {
  let assert Ok(_) = glisten.send(conn, stream |> bytes_tree.from_bit_array)
  subject
}
