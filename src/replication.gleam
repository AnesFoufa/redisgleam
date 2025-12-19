import command
import database
import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import mug
import resp

/// Perform the replica handshake with the master server
pub fn perform_handshake(config: database.Config, db: database.Database) -> Nil {
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
  let assert Ok(psync_and_rdb) = mug.receive(socket, timeout_milliseconds: 1000)
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
          case rest {
            <<_rdb:bytes-size(length), leftover:bits>> -> leftover
            _ -> <<>>
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
  // Process any commands in the initial buffer first (offset starts at 0)
  let #(buffer, offset) = process_buffer(initial_buffer, socket, db, 0)

  // Loop to receive more commands
  receive_loop(socket, db, buffer, offset)
}

fn receive_loop(
  socket: mug.Socket,
  db: database.Database,
  buffer: BitArray,
  offset: Int,
) -> Nil {
  case mug.receive(socket, timeout_milliseconds: 60_000) {
    Ok(data) -> {
      let combined = bit_array.append(buffer, data)
      let #(new_buffer, new_offset) = process_buffer(combined, socket, db, offset)
      receive_loop(socket, db, new_buffer, new_offset)
    }
    Error(_) -> {
      // Timeout or error, just continue waiting
      receive_loop(socket, db, buffer, offset)
    }
  }
}

fn process_buffer(
  buffer: BitArray,
  socket: mug.Socket,
  db: database.Database,
  offset: Int,
) -> #(BitArray, Int) {
  let #(commands, leftover) = resp.parse_all(buffer)

  // Process each command and accumulate the offset
  let final_offset =
    list.fold(commands, offset, fn(current_offset, resp_cmd) {
      // Calculate byte length of this command
      let cmd_bytes = resp.to_bit_array(resp_cmd)
      let cmd_length = bit_array.byte_size(cmd_bytes)

      case command.parse(resp_cmd) {
        Ok(cmd) -> {
          case is_replconf_getack(cmd) {
            True -> {
              // Send ACK with current offset (before adding this GETACK command)
              io.println("Received REPLCONF GETACK, sending ACK with offset: " <> int.to_string(current_offset))
              send_replconf_ack(socket, current_offset)
              // Now increment offset by this GETACK command's byte length
              current_offset + cmd_length
            }
            False -> {
              // Process command silently, then increment offset
              io.println("Processing propagated command, offset: " <> int.to_string(current_offset))
              database.apply_command_silent(db, cmd)
              current_offset + cmd_length
            }
          }
        }
        Error(_) -> {
          io.println("Failed to parse propagated command")
          current_offset
        }
      }
    })

  #(leftover, final_offset)
}

/// Check if a command is REPLCONF GETACK
fn is_replconf_getack(cmd: command.Command) -> Bool {
  case cmd {
    command.ReplConf(command.ReplConfGetAck) -> True
    _ -> False
  }
}

/// Send REPLCONF ACK <offset> response to master
fn send_replconf_ack(socket: mug.Socket, offset: Int) -> Nil {
  // Build RESP array: ["REPLCONF", "ACK", "<offset>"]
  let offset_str = int.to_string(offset)
  let response =
    resp.Array([
      resp.BulkString(<<"REPLCONF":utf8>>),
      resp.BulkString(<<"ACK":utf8>>),
      resp.BulkString(bit_array.from_string(offset_str)),
    ])
    |> resp.to_bit_array()

  case mug.send(socket, response) {
    Ok(_) -> Nil
    Error(_) -> {
      io.println("Failed to send REPLCONF ACK")
      Nil
    }
  }
}
