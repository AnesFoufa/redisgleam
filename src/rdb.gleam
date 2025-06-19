import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/int
import gleam/option.{type Option}
import gleam/result
import gleam/time/timestamp.{type Timestamp}
import parser.{type Parser, bind, return}
import resp.{type Resp}

pub type Item {
  Item(value: Resp, expires_at: Option(Timestamp))
}

pub type Rdb {
  Rdb(
    version: BitArray,
    metadata: List(#(BitArray, BitArray)),
    databases: List(Dict(BitArray, Item)),
  )
}

pub fn from_bit_array(input: BitArray) -> Result(Rdb, BitArray) {
  parser.run(parse_rdb(), input)
  |> result.map_error(fn(error) {
    case error {
      parser.Unexpected(ux) -> ux
      parser.EndOfInput -> <<"End of input">>
    }
  })
}

fn parse_rdb() -> Parser(Rdb) {
  use version <- bind(parse_version())
  echo version
  use metadata <- bind(parse_metadata())
  echo metadata
  use databases <- bind(parse_databases())
  return(Rdb(version, metadata, databases))
}

fn parse_version() -> Parser(BitArray) {
  use _ <- bind(parser.bits(<<"REDIS">>))
  parser.bytes(4)
}

fn parse_metadata() -> Parser(List(#(BitArray, BitArray))) {
  parser.many(parse_metadatum())
}

fn parse_metadatum() -> Parser(#(BitArray, BitArray)) {
  use _ <- bind(parser.bits(<<0xFA>>))
  use name <- bind(parse_string())
  use value <- bind(parse_string())
  return(#(name, value))
}

fn parse_string() -> Parser(BitArray) {
  use size <- bind(parse_size())
  case size {
    Size(s) -> parser.bytes(s)

    StringEncoding(0) -> {
      use bytes <- bind(parser.bytes(1))
      let assert <<i:8>> = bytes
      i |> int.to_string |> bit_array.from_string |> return
    }
    StringEncoding(1) -> {
      use bytes <- bind(parser.bytes(2))
      let assert <<i:size(16)-little>> = bytes
      i |> int.to_string |> bit_array.from_string |> return
    }
    StringEncoding(2) -> {
      use bytes <- bind(parser.bytes(4))
      let assert <<i:size(32)-little>> = bytes
      i |> int.to_string |> bit_array.from_string |> return
    }
    StringEncoding(_x) -> {
      echo size
      panic
    }
  }
}

type Size {
  Size(Int)
  StringEncoding(Int)
}

fn parse_size() -> Parser(Size) {
  use char <- bind(parser.any_char())
  case char {
    <<0b00:2, rest:size(6)>> -> return(Size(rest))
    <<0b01:2, rest:size(6)>> -> {
      use remaining <- bind(parser.any_char())
      let assert <<i:size(14)-big>> = <<rest:6, remaining:bits>>
      return(Size(i))
    }
    <<0b10:2, _rest:bits>> -> {
      use bytes <- bind(parser.bytes(4))
      let assert <<i:size(4)-unit(8)-big>> = bytes
      return(Size(i))
    }
    <<0b11:2, rest:6>> -> return(StringEncoding(rest))
    _ -> {
      echo char
      panic
    }
  }
}

fn parse_databases() -> Parser(List(Dict(BitArray, Item))) {
  use _ <- bind(parser.bits(<<0xFE:8>>))
  parser.many(parse_database())
}

fn parse_database() -> Parser(Dict(BitArray, Item)) {
  use _db_index <- bind(parse_size())
  use _size_section_indicator <- bind(parser.bits(<<0xFB:8>>))
  use _hash_table_size <- bind(parse_size())
  use _expires_size <- bind(parse_size())
  use keys_items <- bind(parser.many(parse_key_item()))
  return(dict.from_list(keys_items))
}

fn parse_key_item() -> Parser(#(BitArray, Item)) {
  use expires_at <- bind(parse_expires_at())
  use _value_type <- bind(parser.any_char())
  use key <- bind(parse_string())
  use value <- bind(parse_string())
  let item = Item(resp.BulkString(value), expires_at)
  return(#(key, item))
}

fn parse_expires_at() -> Parser(Option(Timestamp)) {
  parser.return(option.None)
}
