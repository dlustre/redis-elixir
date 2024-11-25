defmodule Rdb do
  @header "REDIS0011"
  @eof 0xFF
  @selectdb 0xFE
  @string 0x00
  @expiretime 0xFD
  @expiretime_ms 0xFC
  @resizedb 0xFB
  @aux 0xFA

  @next_six <<0b00::2>>
  @next_fourteen <<0b01::2>>
  @next_thirty_two <<0b10::2>>
  @special <<0b11::2>>

  defmodule Section do
    @enforce_keys [:kind, :data]
    defstruct [:kind, :data]
  end

  def empty_file,
    do:
      Base.decode64!(
        "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
      )

  def test_length_encoding() do
    consume(<<0x0A>>, :length_encoded) |> elem(0) |> Util.assert_equals(10)

    consume(<<0x42, 0xBC>>, :length_encoded)
    |> elem(0)
    |> Util.assert_equals(700)

    consume(<<0x80, 0x00, 0x00, 0x42, 0x68>>, :length_encoded)
    |> elem(0)
    |> Util.assert_equals(17000)

    consume(<<0b11::2>>, :length_encoded)
  end

  def test_str_encoding() do
    consume(
      <<0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21>>,
      :str_encoded
    )
    |> elem(0)
    |> Util.assert_equals("Hello, World!")

    consume(
      <<0xC0, 0x7B, 0xFF>>,
      :str_encoded
    )
    |> elem(0)
    |> Util.assert_equals(123)

    consume(
      <<0xC1, 0x39, 0x30>>,
      :str_encoded
    )
    |> elem(0)
    |> Util.assert_equals(12345)

    consume(
      <<0xC2, 0x87, 0xD6, 0x12, 0x00>>,
      :str_encoded
    )
    |> elem(0)
    |> Util.assert_equals(1_234_567)
  end

  def consume(<<@next_six, length::6, tl::binary>>, :length_encoded),
    do: {length, tl} |> IO.inspect(label: "length_encoded: next_six")

  def consume(<<@next_fourteen, length::14, tl::binary>>, :length_encoded),
    do: {length, tl} |> IO.inspect(label: "length_encoded: next_fourteen")

  def consume(<<@next_thirty_two, _::6, length::32, tl::binary>>, :length_encoded),
    do: {length, tl} |> IO.inspect(label: "length_encoded: next_thirty_two")

  def consume(<<@special, str_format::6, tl::binary>>, :length_encoded) do
    case str_format do
      0 -> {:eight, tl}
      1 -> {:sixteen, tl}
      2 -> {:thirtytwo, tl}
    end
  end

  def consume(bin, :str_encoded) do
    case consume(bin, :length_encoded) do
      {:eight, tl} ->
        <<int, tl::binary>> = tl
        {int, tl} |> IO.inspect(label: "str_encoded:eight")

      {:sixteen, tl} ->
        <<int::16-little, tl::binary>> = tl
        {int, tl} |> IO.inspect(label: "str_encoded:sixteen")

      {:thirtytwo, tl} ->
        <<int::32-little, tl::binary>> = tl
        {int, tl} |> IO.inspect(label: "str_encoded:thirtytwo")

      {length, tl} ->
        <<str::size(length)-binary, tl::binary>> = tl
        {str, tl} |> IO.inspect(label: "str_encoded")
    end
  end

  def parse(<<@eof, checksum::64, tl::binary>>) do
    IO.puts("eof")
    {%Section{kind: :eof, data: %{checksum: checksum}}, tl} |> IO.inspect(label: "parsed eof")
  end

  def parse(<<@selectdb, bin::binary>>) do
    IO.puts("selectdb")
    {db_index, tl} = consume(bin, :length_encoded) |> IO.inspect()
    <<@resizedb, tl::binary>> = tl
    {table_size, tl} = consume(tl, :length_encoded) |> IO.inspect()
    {expire_table_size, tl} = consume(tl, :length_encoded) |> IO.inspect()

    {%Section{
       kind: :selectdb,
       data: %{db_index: db_index, table_size: table_size, expire_table_size: expire_table_size}
     }, tl}
  end

  def parse(<<@string, bin::binary>>) do
    IO.puts("string no expiry")
    {key, tl} = consume(bin, :str_encoded) |> IO.inspect()
    {val, tl} = consume(tl, :str_encoded) |> IO.inspect()
    {%Section{kind: :kv_pair, data: %{key: key, val: val}}, tl}
  end

  def parse(<<@expiretime, time::32-little, bin::binary>>) do
    IO.puts("expiretime")
    {key, tl} = consume(bin, :str_encoded) |> IO.inspect()
    {val, tl} = consume(tl, :str_encoded) |> IO.inspect()
    {%Section{kind: :kv_pair, data: %{key: key, val: val, expiretime_ms: time * 1000}}, tl}
  end

  def parse(<<@expiretime_ms, time::64-little, @string, bin::binary>>) do
    IO.puts("expiretime_ms")
    {key, tl} = consume(bin, :str_encoded) |> IO.inspect()
    {val, tl} = consume(tl, :str_encoded) |> IO.inspect()
    {%Section{kind: :kv_pair, data: %{key: key, val: val, expiretime_ms: time}}, tl}
  end

  def parse(<<@aux, bin::binary>>) do
    IO.puts("aux")
    {key, tl} = consume(bin, :str_encoded) |> IO.inspect()
    {val, tl} = consume(tl, :str_encoded) |> IO.inspect()
    {%Section{kind: :aux, data: %{key: key, val: val}}, tl}
  end

  def parse(<<@header, tl::binary>>) do
    IO.puts("header")
    {%Section{kind: :header, data: nil}, tl}
  end

  def parse_file(bin, acc) do
    case parse(bin) do
      {%Section{kind: :eof} = section, tl} -> {Enum.reverse([section | acc]), tl}
      {section, tl} -> parse_file(tl, [section |> IO.inspect(label: "Parsed section") | acc])
    end
  end
end
