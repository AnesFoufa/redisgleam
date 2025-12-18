import gleam/bit_array
import gleam/int
import gleam/list
import parser.{type Parser, bind, fail, return}

pub type Resp {
  SimpleString(BitArray)
  SimpleError(BitArray)
  Integer(Int)
  BulkString(BitArray)
  Array(List(Resp))
  Null
}

pub fn from_bit_array(msg: BitArray) -> Result(Resp, BitArray) {
  case parser.run(parse_resp(), msg) {
    Ok(resp) -> Ok(resp)
    Error(parser.Unexpected(ux)) -> Error(ux)
    Error(parser.EndOfInput) -> Error(<<"End of input">>)
  }
}

/// Parse all RESP messages from a buffer, returning the list of parsed messages
/// and any remaining bytes that couldn't be parsed (incomplete message)
pub fn parse_all(msg: BitArray) -> #(List(Resp), BitArray) {
  do_parse_all(msg, [])
}

fn do_parse_all(msg: BitArray, acc: List(Resp)) -> #(List(Resp), BitArray) {
  case msg {
    <<>> -> #(list.reverse(acc), <<>>)
    _ -> {
      case parser.run_with_rest(parse_resp(), msg) {
        Ok(#(resp, rest)) -> do_parse_all(rest, [resp, ..acc])
        Error(_) -> #(list.reverse(acc), msg)
      }
    }
  }
}

pub fn to_bit_array(resp: Resp) -> BitArray {
  case resp {
    SimpleString(ba) -> <<"+", ba:bits, "\r\n">>
    SimpleError(ba) -> <<"-", ba:bits, "\r\n">>
    Integer(i) -> {
      let str_i = int.to_string(i)
      <<":", str_i:utf8, "\r\n">>
    }
    BulkString(ba) -> {
      let length = bit_array.byte_size(ba) |> int.to_string
      <<"$", length:utf8, "\r\n", ba:bits, "\r\n">>
    }
    Array(resps) -> {
      let ba_resps = list.map(resps, to_bit_array) |> bit_array.concat
      let length = list.length(resps) |> int.to_string
      <<"*", length:utf8, "\r\n", ba_resps:bits>>
    }
    Null -> <<"$-1\r\n">>
  }
}

fn parse_resp() -> Parser(Resp) {
  parser.choice([
    parse_simple_string(),
    parse_simple_error(),
    parse_integer(),
    parse_bulk_string(),
    parse_array(),
  ])
}

fn parse_simple_string() -> Parser(Resp) {
  use _ <- bind(parser.bits(<<"+">>))
  use bit_arrays <- bind(
    parser.many(
      parser.satisfy(parser.any_char(), fn(s) { s != <<"\r">> && s != <<"\n">> }),
    ),
  )
  use _ <- bind(parser.bits(<<"\r\n">>))
  bit_arrays |> bit_array.concat |> SimpleString |> return
}

fn parse_simple_error() -> Parser(Resp) {
  use _ <- bind(parser.bits(<<"-">>))
  use bit_arrays <- bind(
    parser.many(
      parser.satisfy(parser.any_char(), fn(s) { s != <<"\r">> && s != <<"\n">> }),
    ),
  )
  use _ <- bind(parser.bits(<<"\r\n">>))
  bit_arrays |> bit_array.concat |> SimpleError |> return
}

fn parse_integer() -> Parser(Resp) {
  use _ <- bind(parser.bits(<<":">>))
  use int <- bind(parse_int())
  use _ <- bind(parser.bits(<<"\r\n">>))
  return(Integer(int))
}

fn parse_bulk_string() -> Parser(Resp) {
  use _ <- bind(parser.bits(<<"$">>))
  use length <- bind(digits())
  use _ <- bind(parser.bits(<<"\r\n">>))
  case length {
    _ if length < 0 -> {
      use _ <- bind(parser.bits(<<"\r\n">>))
      return(Null)
    }
    _ -> {
      use chars <- bind(parser.bytes(length))
      use _ <- bind(parser.bits(<<"\r\n">>))
      return(BulkString(chars))
    }
  }
}

fn parse_array() -> Parser(Resp) {
  use _ <- bind(parser.bits(<<"*">>))
  use length <- bind(digits())
  use _ <- bind(parser.bits(<<"\r\n">>))
  use elems <- bind(parser.at_most(length, parse_resp()))
  case length == list.length(elems) {
    True -> return(Array(elems))
    False -> fail()
  }
}

fn parse_int() -> Parser(Int) {
  use multiplier <- bind(
    parser.choice([parse_plus(), parse_minus(), parse_nothing()]),
  )
  use int <- bind(digits())
  return(int * multiplier)
}

fn parse_plus() -> Parser(Int) {
  use _ <- bind(parser.bits(<<"+">>))
  return(1)
}

fn parse_minus() -> Parser(Int) {
  use _ <- bind(parser.bits(<<"-">>))
  return(-1)
}

fn parse_nothing() -> Parser(Int) {
  return(1)
}

fn digits() -> Parser(Int) {
  let is_digit = fn(char) {
    case char {
      <<c:8>> if 0x30 <= c && c <= 0x39 -> True
      _ -> False
    }
  }
  let not_empty = fn(xs) {
    case xs {
      [] -> fail()
      _ -> return(xs)
    }
  }
  let bit_array_to_int = fn(ba) {
    let assert Ok(str) = bit_array.to_string(ba)
    let assert Ok(integer) = int.parse(str)
    integer
  }
  parser.any_char()
  |> parser.satisfy(is_digit)
  |> parser.many
  |> parser.bind(not_empty)
  |> parser.map(bit_array.concat)
  |> parser.map(bit_array_to_int)
}
