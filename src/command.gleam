import database.{type Config, type Database}
import gleam/bit_array
import gleam/int
import gleam/option.{type Option}
import gleam/result
import gleam/string
import resp.{type Resp}

pub type Command {
  Ping
  Echo(Resp)
  Get(key: BitArray)
  Set(key: BitArray, value: Resp, duration: Option(Int))
  GetConfigDir
  GetConfigDbFileName
  Keys
  InfoReplication
  ReplConf(args: List(Resp))
  Psync
}

pub fn parse(input: Resp) -> Result(Command, resp.Resp) {
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
    "set", [resp.BulkString(key), value] -> Ok(Set(key, value, option.None))
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

pub fn handle(config: Config, db: Database, command: Command) -> resp.Resp {
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
    Psync -> database.psync(db)
    Keys -> database.keys(db)
  }
}
