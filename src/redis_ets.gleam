import gleam/erlang/atom.{type Atom}
import gleam/list
import gleam/option.{type Option}
import rdb.{type Item}

// ETS wrapper using FFI to Erlang's :ets module

/// Create a new ETS table with default options
pub fn new(name: String) -> Atom {
  let table_name = atom.create_from_string(name)
  do_new_with_options(table_name)
}

// Helper that calls Erlang directly with hardcoded options
@external(erlang, "redis_ets_ffi", "new_table")
fn do_new_with_options(name: Atom) -> Atom

/// Insert a key-value pair into the ETS table
pub fn insert(table: Atom, key: BitArray, value: Item) -> Nil {
  let _ = do_insert(table, #(key, value))
  Nil
}

@external(erlang, "ets", "insert")
fn do_insert(table: Atom, entry: #(BitArray, Item)) -> Bool

/// Lookup a value by key
pub fn lookup(table: Atom, key: BitArray) -> Option(Item) {
  case do_lookup(table, key) {
    [] -> option.None
    [#(_key, value)] -> option.Some(value)
    _ -> option.None
  }
}

@external(erlang, "ets", "lookup")
fn do_lookup(table: Atom, key: BitArray) -> List(#(BitArray, Item))

/// Delete a key from the table
pub fn delete(table: Atom, key: BitArray) -> Nil {
  let _ = do_delete(table, key)
  Nil
}

@external(erlang, "ets", "delete")
fn do_delete(table: Atom, key: BitArray) -> Bool

/// Get all keys from the table
pub fn all_keys(table: Atom) -> List(BitArray) {
  do_tab2list(table)
  |> list.map(fn(entry) {
    let #(key, _value) = entry
    key
  })
}

@external(erlang, "ets", "tab2list")
fn do_tab2list(table: Atom) -> List(#(BitArray, Item))
