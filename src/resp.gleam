import gleam/bit_array
import gleam/int
import gleam/list
import gleam/string
import party.{type Parser, do, go, return}

pub type Resp {
  SimpleString(String)
  SimpleError(String)
  Integer(Int)
  BulkString(String)
  Array(List(Resp))
  Null
}

pub fn from_bit_array(msg: BitArray) -> Result(Resp, String) {
  let assert Ok(msg) = msg |> bit_array.to_string()
  case go(parse_resp(), msg) {
    Ok(resp) -> Ok(resp)
    Error(party.Unexpected(_, error: error)) -> Error(error)
    Error(party.UserError(_, error: error)) -> Error(error)
  }
}

pub fn to_bit_array(resp: Resp) -> BitArray {
  resp |> to_string |> bit_array.from_string
}

fn to_string(resp: Resp) -> String {
  case resp {
    SimpleString(s) -> "+" <> s <> "\r\n"
    SimpleError(s) -> "-" <> s <> "\r\n"
    Integer(i) -> ":" <> int.to_string(i) <> "\r\n"
    BulkString(s) ->
      "$" <> { string.length(s) |> int.to_string } <> "\r\n" <> s <> "\r\n"
    Array(elems) ->
      "*"
      <> { list.length(elems) |> int.to_string }
      <> "\r\n"
      <> { list.map(elems, to_string) |> string.concat }
    Null -> "$-1\r\n"
  }
}

fn parse_resp() -> Parser(Resp, String) {
  party.choice([
    parse_simple_string(),
    parse_simple_error(),
    parse_integer(),
    parse_bulk_string(),
    parse_array(),
  ])
}

fn parse_simple_string() -> Parser(Resp, String) {
  use _ <- do(party.string("+"))
  use str <- do(
    party.many_concat(party.satisfy(fn(s) { s != "\r" && s != "\n" })),
  )
  use _ <- do(party.string("\r\n"))
  return(SimpleString(str))
}

fn parse_simple_error() -> Parser(Resp, String) {
  use _ <- do(party.string("-"))
  use str <- do(
    party.many_concat(party.satisfy(fn(s) { s != "\r" && s != "\n" })),
  )
  use _ <- do(party.string("\r\n"))
  return(SimpleError(str))
}

fn parse_integer() -> Parser(Resp, String) {
  use _ <- do(party.string(":"))
  use int <- do(parse_int())
  use _ <- do(party.string("\r\n"))
  return(Integer(int))
}

fn parse_bulk_string() -> Parser(Resp, String) {
  use _ <- do(party.string("$"))
  use length <- do(parse_int())
  use _ <- do(party.string("\r\n"))
  case length {
    _ if length < 0 -> {
      use _ <- do(party.string("\r\n"))
      return(Null)
    }
    _ -> {
      use chars <- do(parse_n([], length))
      let chars = list.reverse(chars)
      let str = string.concat(chars)
      use _ <- do(party.string("\r\n"))
      return(BulkString(str))
    }
  }
}

fn parse_array() -> Parser(Resp, String) {
  use _ <- do(party.char("*"))
  use length <- do(parse_int())
  use _ <- do(party.string("\r\n"))
  use elems <- do(party.many(parse_resp()))
  case length == list.length(elems) {
    True -> return(Array(elems))
    False -> party.fail()
  }
}

fn parse_int() -> Parser(Int, String) {
  use multiplier <- do(
    party.choice([parse_plus(), parse_minus(), parse_nothing()]),
  )
  use int_str <- do(party.digits())
  let assert Ok(int) = int.parse(int_str)
  return(int * multiplier)
}

fn parse_plus() -> Parser(Int, String) {
  use _ <- do(party.string("+"))
  return(1)
}

fn parse_minus() -> Parser(Int, String) {
  use _ <- do(party.string("-"))
  return(-1)
}

fn parse_nothing() -> Parser(Int, String) {
  return(1)
}

fn parse_n(chars: List(String), n: Int) -> Parser(List(String), String) {
  case n {
    0 -> return(chars)
    _ -> {
      use char <- do(party.any_char())
      parse_n([char, ..chars], n - 1)
    }
  }
}
