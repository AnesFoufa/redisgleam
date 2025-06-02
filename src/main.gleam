import gleam/bytes_builder
import gleam/io

import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten

pub fn main() {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  io.println("Logs from your program will appear here!")

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(_msg, state, conn) {
      io.println("Received message!")
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string("+PONG\r\n"))
      actor.continue(state)
    })
    |> glisten.serve(6379)

  process.sleep_forever()
}
