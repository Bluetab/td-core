defmodule TdCore.Utils.FileHashTest do
  use ExUnit.Case

  alias TdCore.Utils.FileHash

  describe "FileHash.hash/2" do
    test "generates a base 16 string" do
      filepath = "test/fixtures/test_file.txt"
      expected_hash = "8985926CD4BC8B7C0598231FF748E1C3"
      assert FileHash.hash(filepath, :md5) == expected_hash
    end
  end
end
