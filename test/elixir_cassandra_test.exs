defmodule ElixirCassandraTest do
  use ExUnit.Case
  doctest ElixirCassandra

  alias CQEx.Query, as: Query

  require IEx

  # Cassandra
  # The Partition Key is responsible for data distribution across your nodes.
  # The Clustering Key is responsible for data sorting within the partition.
  # The Primary Key is equivalent to the Partition Key in a single-field-key table.
  # The Composite/Compound Key is just a multiple-columns key
  # http://stackoverflow.com/questions/24949676/difference-between-partition-key-composite-key-and-clustering-key-in-cassandra
  # http://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html

  # ExUnit
  # http://elixir-lang.org/docs/stable/ex_unit/ExUnit.Callbacks.html

  setup do
    client = CQEx.Client.new!

    [client: client]
  end

  describe "when the table does not exist" do
    test "it will throw an error", context do
      assert catch_error(
        context[:client]
        |> Query.call!("SELECT * FROM foobar;")
      ) == %CQEx.Error{
        message: "\"unconfigured table foobar (Code 8704)\"",
        stack: []
      }
    end
  end


  describe "when the primary key is simple (aka the primary key is the partition key)" do
    setup context do
      context[:client] |> Query.call!("""
        CREATE TABLE IF NOT EXISTS users (
          user_name varchar PRIMARY KEY,
          password varchar,
          gender varchar,
          session_token varchar,
          state varchar,
          birth_year bigint
        );
      """)

      :ok
    end

    test "it will throw an error if inserting without the primary key", context do
      assert catch_error(
        context[:client]
        |> Query.call!(%Query{
          statement: "INSERT INTO users (password) values (?);",
          values: %{
            password: "foo"
          }
        })
      ) == %CQEx.Error{
        message: "\"Some partition key parts are missing: user_name (Code 8704)\"",
        stack: []
      }
    end

    test "it inserts and can query using the primary key", context do
      context[:client]
      |> Query.call!(%Query{
        statement: "INSERT INTO users (user_name, password) values (?, ?);",
        values: %{
          user_name: "foo",
          password: "foo"
        }
      })

      assert context[:client]
      |> Query.call!("SELECT user_name, password FROM users WHERE user_name = 'foo';")
      |> Enum.to_list == [[user_name: "foo", password: "foo"]]
    end
  end

  describe "primary key is composite/compound (the partition key is the first part and the clustering key is the second part)" do
    setup context do
      context[:client] |> Query.call!("""
        CREATE TABLE IF NOT EXISTS emp (
          empID int,
          deptID int,
          first_name varchar,
          last_name varchar,
          PRIMARY KEY (empID, deptID)
        );
      """)

      :ok
    end

    test "it will throw an error if the partition key and clustering ID are not provided", context do
      assert catch_error(
        context[:client]
        |> Query.call!(%Query{
          statement: "INSERT INTO emp (empID) values (?);",
          values: %{
            empid: 1
          }
        })
      ) == %CQEx.Error{
        message: "\"Some clustering keys are missing: deptid (Code 8704)\"",
        stack: []
      }
    end

    test "it inserts and can query using the primary key and cluster key", context do
      context[:client]
      |> Query.call!(%Query{
        statement: "INSERT INTO emp (empID, deptID) values (?, ?);",
        values: %{
          empid: 123,
          deptid: 456
        }
      })

      assert context[:client]
      |> Query.call!("SELECT first_name, deptID FROM emp WHERE empID = 123;")
      |> Enum.to_list == [[first_name: nil, deptid: 456]]
    end
  end



  describe "primary key is composite/compound with multiple parts" do
    # note: the partion key is the first group, cluster key is the second group
    setup context do
      context[:client] |> Query.call!("""
        CREATE TABLE IF NOT EXISTS Cats (
          block_id uuid,
          breed text,
          color text,
          short_hair boolean,
          PRIMARY KEY ((block_id, breed), color, short_hair)
        );
      """)

      :ok
    end


    test "it will throw an error if the partition key and clustering ID are not provided", context do
      assert catch_error(
        context[:client]
        |> Query.call!(%Query{
          statement: "INSERT INTO Cats (block_id, color) values (?, ?);",
          values: %{
            block_id: 123,
            color: "green"
          }
        })
      ) == %CQEx.Error{
        message: "\"Some partition key parts are missing: breed (Code 8704)\"",
        stack: []
      }
    end

    test "it inserts and can query using the cluster key", context do

      {uuid, _} = :uuid.get_v1(:uuid.new(self(), :os))
      uuid = :uuid.uuid_to_string(uuid, :standard)

      context[:client]
      |> Query.call!(%Query{
        statement: "INSERT INTO Cats (block_id, breed, color, short_hair) values (?, ?, ?, ?);",
        values: %{
          block_id: uuid,
          breed: "cat",
          color: "green",
          short_hair: true
        }
      })

      assert context[:client]
      |> Query.call!("SELECT breed FROM cats WHERE block_id=#{uuid} AND breed = 'cat';")
      |> Enum.to_list == [[breed: "cat"]]
    end
  end


  describe "clustering order" do
    setup context do
      context[:client] |> Query.call!("""
        CREATE TABLE IF NOT EXISTS timeseries (
          event_type text,
          insertion_time timestamp,
          event blob,
          PRIMARY KEY (event_type, insertion_time)
        )
        WITH CLUSTERING ORDER BY (insertion_time DESC);
      """)

      :ok
    end


    test "it will throw an error when clustering key is missing", context do
      assert catch_error(
        context[:client]
        |> Query.call!(%Query{
            statement: "INSERT INTO timeseries (event_type) values (?);",
            values: %{
              event_type: "foo"
            }
          })
      ) == %CQEx.Error{
        message: "\"Some clustering keys are missing: insertion_time (Code 8704)\"",
        stack: []
      }
    end

    test "it inserts and can query using the cluster key", context do

      context[:client]
      |> Query.call!(%Query{
          statement: "INSERT INTO timeseries (event_type, insertion_time) values (?, ?);",
          values: %{
            event_type: "foo",
            insertion_time: :now
          }
        })

      assert context[:client]
      |> Query.call!("SELECT event_type FROM timeseries WHERE event_type='foo' LIMIT 1;") # limit 1 as the primary key is based on both the event type and the time the spec is ran
      |> Enum.to_list == [[event_type: "foo"]]
    end
  end
end
