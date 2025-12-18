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
        resp.BulkString(option_name),
        resp.BulkString(duration),
      ]
    -> {
      use option_name <- result.try(
        option_name
        |> bit_array.to_string()
        |> result.replace_error(
          resp.SimpleError(<<"Expected a valid string as option name">>),
        ),
      )
      case string.lowercase(option_name) {
        "px" -> {
          let duration =
            duration
            |> bit_array.to_string
            |> result.try(int.parse)
            |> option.from_result
          Ok(Set(key, value, duration))
        }
        _ -> Error(resp.SimpleError(<<"Unknown set option">>))
      }
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
