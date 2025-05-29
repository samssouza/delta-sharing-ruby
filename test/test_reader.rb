require 'test_helper'

class TestDeltaSharingReader < Minitest::Test
  def setup
    @profile_file = File.join(File.dirname(__FILE__), 'fixtures', 'test_profile.json')
    @client = DeltaSharing::Client.new(@profile_file)
    @endpoint = 'https://example.com/delta-sharing'
    @bearer_token = 'test-token'
    @table_name = 'test_share.test_schema.test_table'
    @reader = DeltaSharing::Reader.new(table: @table_name, client: @client)

    # Create sample Arrow table data
    @sample_arrow_table = create_sample_arrow_table
    @sample_parquet_data = create_parquet_data(@sample_arrow_table)

    @sample_multiple_files_arrow_table = create_sample_multiple_files_arrow_table

    @sample_arrow_table_with_partition = create_sample_arrow_table_with_partition
    @sample_parquet_data_with_partition = create_parquet_data(@sample_arrow_table_with_partition)
  end

  def test_initialize_with_valid_table_format
    reader = DeltaSharing::Reader.new(table: 'share.schema.table', client: @client)
    assert_equal 'share', reader.share
    assert_equal 'schema', reader.schema
    assert_equal 'table', reader.name
    assert_equal @client, reader.client
  end

  def test_initialize_with_invalid_table_format
    assert_raises(ArgumentError) do
      DeltaSharing::Reader.new(table: 'invalid_format', client: @client)
    end

    assert_raises(ArgumentError) do
      DeltaSharing::Reader.new(table: 'share.table', client: @client)
    end

    assert_raises(ArgumentError) do
      DeltaSharing::Reader.new(table: 'share.schema.table.extra', client: @client)
    end
  end

  def test_load_simple_table_as_arrow
    mock_simple_table_request
    mock_parquet_file1_request

    arrow_table = @reader.load_as_arrow

    assert_instance_of Arrow::Table, arrow_table
    assert_equal @sample_arrow_table, arrow_table
  end

  def test_load_as_arrow_with_partitions
    mock_partitioned_table_request
    mock_parquet_file_with_partitions_request

    arrow_table = @reader.load_as_arrow

    assert_instance_of Arrow::Table, arrow_table
    assert_equal @sample_arrow_table_with_partition, arrow_table
  end

  def test_load_as_arrow_with_multiple_files
    mock_multiple_files_table_request
    mock_parquet_file1_request
    mock_parquet_file2_request

    arrow_table = @reader.load_as_arrow

    assert_instance_of Arrow::Table, arrow_table
    assert_equal @sample_multiple_files_arrow_table, arrow_table
  end

  def test_load_as_arrow_with_options
    options = {
      limit: 10,
      predicate_hints: ['age > 25'],
      version: 1
    }

    mock_table_request_with_options(options)
    mock_parquet_file1_request

    arrow_table = @reader.load_as_arrow(options)

    assert !arrow_table.nil?
    assert_instance_of Arrow::Table, arrow_table
  end

  def test_invalid_json_response
    stub_request(:post, "#{@endpoint}/shares/test_share/schemas/test_schema/tables/test_table/query")
      .to_return(
        status: 200,
        body: "invalid json\n{\"metaData\": {}}"
      )

    assert_raises(DeltaSharing::ProtocolError) do
      @reader.load_as_arrow
    end
  end

  def test_network_error_on_file_download
    mock_simple_table_request

    # Mock failed file download
    stub_request(:get, 'https://example-bucket.s3.amazonaws.com/test-file.parquet')
      .to_return(status: 500, body: 'Internal Server Error')

    assert_raises(DeltaSharing::NetworkError) do
      @reader.load_as_arrow
    end
  end

  def test_validation_methods
    # Test predicate hints validation
    assert_raises(ArgumentError) do
      @reader.send(:validate_predicate_hints, 'not an array')
    end

    assert_raises(ArgumentError) do
      @reader.send(:validate_predicate_hints, [123, 'valid'])
    end

    # Test JSON predicate hints validation
    assert_raises(ArgumentError) do
      @reader.send(:validate_json_predicate_hints, 123)
    end

    assert_raises(ArgumentError) do
      @reader.send(:validate_json_predicate_hints, 'invalid json')
    end

    # Test limit validation
    assert_raises(ArgumentError) do
      @reader.send(:validate_limit, -1)
    end

    assert_raises(ArgumentError) do
      @reader.send(:validate_limit, 'not a number')
    end

    # Test version validation
    assert_raises(ArgumentError) do
      @reader.send(:validate_version, -1)
    end
  end

  private

  def create_sample_arrow_table
    id_array = Arrow::Int64Array.new([1, 2, 3])
    name_array = Arrow::StringArray.new(%w[Alice Bob Charlie])

    Arrow::Table.new(
      'id' => id_array,
      'name' => name_array
    )
  end

  def create_sample_multiple_files_arrow_table
    arrow_table1 = create_sample_arrow_table
    arrow_table2 = create_sample_arrow_table

    Arrow::Table.new(arrow_table1.schema, [arrow_table1.each_record_batch.to_a,
                                           arrow_table2.each_record_batch.to_a].flatten)
  end

  def create_sample_arrow_table_with_partition
    arrow_table = create_sample_arrow_table
    active_array = Arrow::BooleanArray.new([true, true, true])

    arrow_table.merge('active' => active_array)
  end

  def create_parquet_data(arrow_table)
    Tempfile.create(['test', '.parquet']) do |temp_file|
      temp_file.binmode
      arrow_table.save(temp_file.path)
      temp_file.rewind
      return temp_file.read
    end
  end

  def mock_simple_table_request
    response_body = [
      protocol_json,
      metadata_json,
      file1_json
    ].join("\n")

    stub_request(:post, "#{@endpoint}/shares/test_share/schemas/test_schema/tables/test_table/query")
      .to_return(status: 200, body: response_body)
  end

  def mock_partitioned_table_request
    response_body = [
      protocol_json,
      metadata_with_partitions_json,
      file1_with_partitions_json
    ].join("\n")

    stub_request(:post, "#{@endpoint}/shares/test_share/schemas/test_schema/tables/test_table/query")
      .to_return(status: 200, body: response_body)
  end

  def mock_multiple_files_table_request
    response_body = [
      protocol_json,
      metadata_json,
      file1_json,
      file2_json
    ].join("\n")

    stub_request(:post, "#{@endpoint}/shares/test_share/schemas/test_schema/tables/test_table/query")
      .to_return(status: 200, body: response_body)
  end

  def mock_table_request_with_options(options)
    stub_request(:post, "#{@endpoint}/shares/test_share/schemas/test_schema/tables/test_table/query")
      .with(
        body: hash_including(
          'limitHint' => options[:limit],
          'predicateHints' => options[:predicate_hints],
          'version' => options[:version]
        )
      )
      .to_return(status: 200, body: [protocol_json, metadata_json, file1_json].join("\n"))
  end

  def mock_parquet_file1_request
    stub_request(:get, JSON.parse(file1_json)['file']['url'])
      .to_return(status: 200, body: @sample_parquet_data)
  end

  def mock_parquet_file2_request
    stub_request(:get, JSON.parse(file2_json)['file']['url'])
      .to_return(status: 200, body: @sample_parquet_data)
  end

  def mock_parquet_file_with_partitions_request
    stub_request(:get, JSON.parse(file1_with_partitions_json)['file']['url'])
      .to_return(status: 200, body: @sample_parquet_data_with_partition)
  end

  def protocol_json
    {
      'protocol' => {
        'minReaderVersion' => 1
      }
    }.to_json
  end

  def metadata_json
    {
      'metaData' => {
        'id' => 'test-table-id',
        'format' => { 'provider' => 'parquet' },
        'schemaString' => {
          'type' => 'struct',
          'fields' => [
            { 'name' => 'id', 'type' => 'long', 'nullable' => true, 'metadata' => {} },
            { 'name' => 'name', 'type' => 'string', 'nullable' => true, 'metadata' => {} }
          ]
        }.to_json,
        'partitionColumns' => [],
        'configuration' => {}
      }
    }.to_json
  end

  def metadata_with_partitions_json
    {
      'metaData' => {
        'id' => 'test-table-id',
        'format' => { 'provider' => 'parquet' },
        'schemaString' => {
          'type' => 'struct',
          'fields' => [
            { 'name' => 'id', 'type' => 'long', 'nullable' => true, 'metadata' => {} },
            { 'name' => 'name', 'type' => 'string', 'nullable' => true, 'metadata' => {} },
            { 'name' => 'active', 'type' => 'boolean', 'nullable' => true, 'metadata' => {} }
          ]
        }.to_json,
        'partitionColumns' => ['active'],
        'configuration' => {}
      }
    }.to_json
  end

  def file1_json
    {
      'file' => {
        'url' => 'https://example-bucket.s3.amazonaws.com/test-file.parquet',
        'id' => 'test-file-id',
        'size' => 1024,
        'partitionValues' => {},
        'stats' => '{"numRecords":3}'
      }
    }.to_json
  end

  def file1_with_partitions_json
    {
      'file' => {
        'url' => 'https://example-bucket.s3.amazonaws.com/test-file-partitioned.parquet',
        'id' => 'test-file-partitioned-id',
        'size' => 1024,
        'partitionValues' => { 'active' => 'true' },
        'stats' => '{"numRecords":3}'
      }
    }.to_json
  end

  def file2_json
    {
      'file' => {
        'url' => 'https://example-bucket.s3.amazonaws.com/test-file-2.parquet',
        'id' => 'test-file-2-id',
        'size' => 1024,
        'partitionValues' => {},
        'stats' => '{"numRecords":3}'
      }
    }.to_json
  end
end
