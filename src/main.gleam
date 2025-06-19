import argv
import clad
import database.{type Config}
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
    option.Some(replica_info) -> {
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
                  resp.BulkString(
                    bit_array.from_string(int.to_string(config.port)),
                  ),
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
    option.None -> io.println("No replica configuration provided")
  }
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      io.println("Received message!")
      let assert glisten.Packet(msg) = msg
      let input = from_bit_array(msg)
      let response =
        input
        |> result.map(parse_command)
        |> result.map_error(resp.SimpleError)
        |> result.flatten()
        |> result.map(fn(command) { handle_command(config, db, command) })
        |> result.unwrap_both()
        |> resp.to_bit_array()
        |> bytes_tree.from_bit_array()

      let assert Ok(_) = glisten.send(conn, response)
      actor.continue(state)
    })
    |> glisten.serve(config.port)

  process.sleep_forever()
}

type Command {
  Ping
  Echo(resp.Resp)
  Get(key: BitArray)
  Set(key: BitArray, value: resp.Resp, duration: option.Option(Int))
  GetConfigDir
  GetConfigDbFileName
  Keys
  InfoReplication
  ReplConf(args: List(resp.Resp))
  Psync
}

fn parse_command(input: resp.Resp) -> Result(Command, resp.Resp) {
  use #(command_name, args) <- result.try(case input {
    resp.Array([resp.BulkString(command_name), ..args]) ->
      Ok(#(command_name, args))
    _ -> resp.SimpleError(<<"Expected a non empty array">>) |> Error
  })
  use command_name <- result.try(
    bit_array.to_string(command_name)
    |> result.map_error(fn(_) {
      resp.SimpleError(<<"Expected a valid string as command name">>)
    }),
  )
  case command_name |> string.lowercase(), args {
    "ping", [] -> Ok(Ping)
    "echo", [value] -> Ok(Echo(value))
    "get", [resp.BulkString(key)] -> Ok(Get(key))
    "set", [resp.BulkString(key), value] -> Ok(Set(key, value, None))
    "set",
      [
        resp.BulkString(key),
        value,
        resp.BulkString(<<"px">>),
        resp.BulkString(duration),
      ]
    -> {
      let duration =
        duration
        |> bit_array.to_string
        |> result.try(int.parse)
        |> option.from_result
      Ok(Set(key, value, duration))
    }
    "config", [resp.BulkString(config_command), ..args] -> {
      case bit_array.to_string(config_command) {
        Ok(command_name) -> parse_config_command(command_name, args)
        Error(_) ->
          Error(resp.SimpleError(<<"Config command not a string!!!">>))
      }
    }
    "info", [resp.BulkString(section)] -> {
      use section_str <- result.try(
        bit_array.to_string(section)
        |> result.map_error(fn(_) {
          resp.SimpleError(<<"Expected a valid string as section name">>)
        }),
      )
      case string.lowercase(section_str) {
        "replication" -> Ok(InfoReplication)
        _ -> Error(resp.SimpleError(<<"Unknown info section">>))
      }
    }
    "keys", _ -> Ok(Keys)
    "replconf", args -> Ok(ReplConf(args))
    "psync", _ -> Ok(Psync)
    _, _ -> Error(resp.SimpleError(<<"Unknown command">>))
  }
}

fn parse_config_command(
  command_name: String,
  args: List(resp.Resp),
) -> Result(Command, resp.Resp) {
  case string.lowercase(command_name), args {
    "get", [resp.BulkString(config_parameter)] -> {
      use config_parameter_name <- result.try(
        config_parameter
        |> bit_array.to_string()
        |> result.replace_error(
          resp.SimpleError(<<"Config parameter not a string">>),
        ),
      )
      case string.lowercase(config_parameter_name) {
        "dir" -> Ok(GetConfigDir)
        "db_filename" -> Ok(GetConfigDbFileName)
        _ -> Error(resp.SimpleError(<<"Unexpected config parameter">>))
      }
    }
    _, _ -> Error(resp.SimpleError(<<"Unknown config command">>))
  }
}

fn handle_command(
  config: Config,
  db: database.Database,
  command: Command,
) -> resp.Resp {
  let repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
  case command {
    Ping -> resp.SimpleString(<<"PONG">>)
    Echo(value) -> value
    Get(key) -> database.get(db, key)
    Set(key, value, duration) -> database.set(db, key, value, duration)
    GetConfigDbFileName ->
      resp.Array([
        resp.BulkString(<<"db_filename">>),
        resp.BulkString(bit_array.from_string(config.db_filename)),
      ])
    GetConfigDir ->
      resp.Array([
        resp.BulkString(<<"dir">>),
        resp.BulkString(bit_array.from_string(config.dir)),
      ])
    InfoReplication -> {
      case config.replicaof {
        option.Some(_) -> resp.BulkString(bit_array.from_string("role:slave"))
        option.None ->
          resp.BulkString(bit_array.from_string(
            "role:master\r\nmaster_replid:"
            <> repl_id
            <> "\r\nmaster_repl_offset:0",
          ))
      }
    }
    ReplConf(_args) -> resp.SimpleString(<<"OK">>)
    Psync ->
      resp.SimpleString(bit_array.from_string("FULLRESYNC " <> repl_id <> " 0"))
    Keys -> database.keys(db)
  }
}
