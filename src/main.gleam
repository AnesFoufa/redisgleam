import argv
import clad
import command
import database
import gleam/bit_array
import gleam/bytes_tree
import gleam/int
import gleam/io
import gleam/list
import gleam/result
import gleam/string

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option
import gleam/otp/actor
import glisten
import mug
import resp

pub fn main() {
  let cli_args = argv.load().arguments
  let assert Ok(config) = clad.decode(cli_args, config_decoder())
  let db = database.start(config)

  replica_handshake(config, db)
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

fn replica_handshake(config: database.Config, db: database.Database) -> Nil {
  case config.replicaof {
    option.Some(#(master_host, master_port)) -> {
      let socket = connect_to_master(master_host, master_port)
      send_ping(socket)
      send_replconf_commands(socket, config.port)
      send_psync(socket)
      let leftover = receive_fullresync(socket)
      start_replication_listener(socket, db, leftover)
      Nil
    }
    option.None -> Nil
  }
}

fn connect_to_master(host: String, port: Int) -> mug.Socket {
  io.println("Connecting to master " <> host <> ":" <> int.to_string(port))
  let assert Ok(socket) =
    mug.new(host, port: port)
    |> mug.timeout(milliseconds: 500)
    |> mug.connect()
  socket
}

fn send_ping(socket: mug.Socket) -> Nil {
  let ping_cmd = resp.Array([resp.BulkString(bit_array.from_string("PING"))])
  let assert Ok(Nil) = mug.send(socket, resp.to_bit_array(ping_cmd))
  let assert Ok(_pong_packet) = mug.receive(socket, timeout_milliseconds: 100)
  Nil
}

fn send_replconf_commands(socket: mug.Socket, port: Int) -> Nil {
  // Send REPLCONF listening-port <PORT> command
  let listening_port_cmd =
    resp.Array([
      resp.BulkString(bit_array.from_string("REPLCONF")),
      resp.BulkString(bit_array.from_string("listening-port")),
      resp.BulkString(bit_array.from_string(int.to_string(port))),
    ])
  let assert Ok(Nil) = mug.send(socket, resp.to_bit_array(listening_port_cmd))
  let assert Ok(_) = mug.receive(socket, timeout_milliseconds: 100)

  // Send REPLCONF capa psync2 command
  let capa_cmd =
    resp.Array([
      resp.BulkString(bit_array.from_string("REPLCONF")),
      resp.BulkString(bit_array.from_string("capa")),
      resp.BulkString(bit_array.from_string("psync2")),
    ])
  let assert Ok(Nil) = mug.send(socket, resp.to_bit_array(capa_cmd))
  let assert Ok(_) = mug.receive(socket, timeout_milliseconds: 100)
  Nil
}

fn send_psync(socket: mug.Socket) -> Nil {
  let psync_cmd =
    resp.Array([
      resp.BulkString(bit_array.from_string("PSYNC")),
      resp.BulkString(bit_array.from_string("?")),
      resp.BulkString(bit_array.from_string("-1")),
    ])
  let assert Ok(Nil) = mug.send(socket, resp.to_bit_array(psync_cmd))
  Nil
}

fn receive_fullresync(socket: mug.Socket) -> BitArray {
  let assert Ok(psync_and_rdb) =
    mug.receive(socket, timeout_milliseconds: 1000)
  skip_fullresync_and_rdb(psync_and_rdb)
}

fn start_replication_listener(
  socket: mug.Socket,
  db: database.Database,
  initial_buffer: BitArray,
) -> Nil {
  process.start(
    fn() { receive_propagated_commands(socket, db, initial_buffer) },
    True,
  )
  Nil
}

/// Skip the FULLRESYNC response and RDB file, return any leftover bytes
fn skip_fullresync_and_rdb(data: BitArray) -> BitArray {
  // Find the end of the FULLRESYNC line
  let data = skip_until_crlf(data)
  // Now we should have $<length>\r\n<rdb_bytes>...
  skip_rdb_file(data)
}

fn skip_until_crlf(data: BitArray) -> BitArray {
  case data {
    <<"\r\n":utf8, rest:bits>> -> rest
    <<_:8, rest:bits>> -> skip_until_crlf(rest)
    _ -> <<>>
  }
}

fn skip_rdb_file(data: BitArray) -> BitArray {
  case data {
    <<"$":utf8, rest:bits>> -> {
      // Parse the length
      let #(length, rest) = parse_rdb_length(rest, 0)
      // Skip past \r\n and the RDB bytes
      case rest {
        <<"\r\n":utf8, rest:bits>> -> {
          case bit_array.byte_size(rest) >= length {
            True -> {
              let assert <<_rdb:bytes-size(length), leftover:bits>> = rest
              leftover
            }
            False -> <<>>
          }
        }
        _ -> <<>>
      }
    }
    _ -> <<>>
  }
}

fn parse_rdb_length(data: BitArray, acc: Int) -> #(Int, BitArray) {
  case data {
    <<c:8, rest:bits>> if c >= 0x30 && c <= 0x39 -> {
      let digit = c - 0x30
      parse_rdb_length(rest, acc * 10 + digit)
    }
    _ -> #(acc, data)
  }
}

/// Receive and process propagated commands from master
fn receive_propagated_commands(
  socket: mug.Socket,
  db: database.Database,
  initial_buffer: BitArray,
) -> Nil {
  // Process any commands in the initial buffer first
  let buffer = process_buffer(initial_buffer, db)

  // Loop to receive more commands
  receive_loop(socket, db, buffer)
}

fn receive_loop(
  socket: mug.Socket,
  db: database.Database,
  buffer: BitArray,
) -> Nil {
  case mug.receive(socket, timeout_milliseconds: 60_000) {
    Ok(data) -> {
      let combined = bit_array.append(buffer, data)
      let new_buffer = process_buffer(combined, db)
      receive_loop(socket, db, new_buffer)
    }
    Error(_) -> {
      // Timeout or error, just continue waiting
      receive_loop(socket, db, buffer)
    }
  }
}

fn process_buffer(buffer: BitArray, db: database.Database) -> BitArray {
  let #(commands, leftover) = resp.parse_all(buffer)
  list.each(commands, fn(resp_cmd) {
    case command.parse(resp_cmd) {
      Ok(cmd) -> {
        io.println("Processing propagated command")
        database.apply_command_silent(db, cmd)
      }
      Error(_) -> {
        io.println("Failed to parse propagated command")
        Nil
      }
    }
  })
  leftover
}
