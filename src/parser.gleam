import gleam/bit_array
import gleam/int
import gleam/list
import gleam/result

pub type ParseError {
  EndOfInput
  Unexpected(BitArray)
}

pub opaque type Parser(a) {
  Parser(parse: fn(BitArray) -> Result(#(a, BitArray), ParseError))
}

pub fn run(parser: Parser(a), input: BitArray) -> Result(a, ParseError) {
  parser.parse(input)
  |> result.map(fn(parse_res) {
    let #(val, _rest) = parse_res
    val
  })
}

pub fn return(val: a) -> Parser(a) {
  Parser(fn(input) { Ok(#(val, input)) })
}

pub fn input() -> Parser(BitArray) {
  Parser(fn(inp) { Ok(#(inp, inp)) })
}

pub fn bind(p: Parser(a), f: fn(a) -> Parser(b)) -> Parser(b) {
  Parser(fn(input) {
    case p.parse(input) {
      Ok(#(val, rest)) -> f(val).parse(rest)
      Error(pe) -> Error(pe)
    }
  })
}

pub fn fail() -> Parser(a) {
  Parser(fn(input) { Error(Unexpected(input)) })
}

pub fn map(p: Parser(a), f: fn(a) -> b) -> Parser(b) {
  Parser(fn(input) {
    case p.parse(input) {
      Ok(#(val, rest)) -> Ok(#(f(val), rest))
      Error(pe) -> Error(pe)
    }
  })
}

pub fn any_char() -> Parser(BitArray) {
  Parser(fn(input) {
    case input {
      <<>> -> Error(EndOfInput)
      <<head:8, rest:bits>> -> {
        Ok(#(<<head:8>>, rest))
      }
      _ -> Error(Unexpected(input))
    }
  })
}

pub fn satisfy(p: Parser(a), f: fn(a) -> Bool) -> Parser(a) {
  let filter = fn(val) {
    case f(val) {
      True -> return(val)
      False -> fail()
    }
  }
  bind(p, filter)
}

pub fn bits(ba: BitArray) -> Parser(BitArray) {
  let ba_size = ba |> bit_array.bit_size
  let assert <<ba_int:size(ba_size)>> = ba
  Parser(fn(input) {
    case input {
      <<head:size(ba_size), rest:bits>> if head == ba_int -> Ok(#(ba, rest))
      _ -> Error(Unexpected(input))
    }
  })
}

pub fn many(p: Parser(a)) -> Parser(List(a)) {
  Parser(fn(input) { Ok(do_many([], input, p)) })
}

fn do_many(acc, input, p: Parser(a)) {
  case p.parse(input) {
    Ok(#(val, rest)) -> do_many([val, ..acc], rest, p)
    Error(_pe) -> #(list.reverse(acc), input)
  }
}

pub fn choice(ps: List(Parser(a))) -> Parser(a) {
  Parser(fn(input) { do_choice(ps, input) })
}

fn do_choice(
  ps: List(Parser(a)),
  input: BitArray,
) -> Result(#(a, BitArray), ParseError) {
  case ps {
    [] -> Error(Unexpected(input))
    [p, ..parsers] -> {
      let parse_result = p.parse(input)
      case parse_result {
        Error(_) -> do_choice(parsers, input)
        _ -> parse_result
      }
    }
  }
}

pub fn bytes(n: Int) -> Parser(BitArray) {
  Parser(fn(input) {
    case input {
      <<head:bytes-size(n), rest:bits>> -> Ok(#(head, rest))
      _ -> Error(Unexpected(input))
    }
  })
}

pub fn at_most(n: Int, p: Parser(a)) -> Parser(List(a)) {
  let n = int.max(n, 0)
  Parser(do_at_most(n, [], p, _))
}

fn do_at_most(n, acc, p: Parser(a), input) {
  case n, p.parse(input) {
    0, _ -> Ok(#(list.reverse(acc), input))
    _, Ok(#(x, rest)) -> do_at_most(n - 1, [x, ..acc], p, rest)
    _, Error(_) -> Ok(#(list.reverse(acc), input))
  }
}
