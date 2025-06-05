import gleam/bytes_tree
import gleam/io

import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten
import resp.{from_bit_array}

pub fn main() {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  io.println("Logs from your program will appear here!")

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      io.println("Received message!")
      let assert glisten.Packet(msg) = msg
      let resp = from_bit_array(msg)
      let response = case resp {
        Ok(resp.Array([_ping])) -> resp.SimpleString("PONG")
        Ok(resp.Array([_echo, arg, ..])) -> arg
        Error(err) -> resp.SimpleError(err)
        _ -> {
          resp.SimpleError("unexpected!!!")
        }
      }
      let assert Ok(_) =
        glisten.send(
          conn,
          response |> resp.to_bit_array |> bytes_tree.from_bit_array,
        )
      actor.continue(state)
    })
    |> glisten.serve(6379)

  process.sleep_forever()
}
