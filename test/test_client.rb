require 'test_helper'

class TestDeltaSharingClient < Minitest::Test
  def setup
    @profile_path = File.join(File.dirname(__FILE__), 'fixtures', 'test_profile.json')
    @profile = JSON.parse(File.read(@profile_path))
    @client = DeltaSharing::Client.new(@profile_path)
    @endpoint = @profile['endpoint']
    @bearer_token = @profile['bearerToken']
  end

  def test_initialize_with_valid_profile
    assert_instance_of DeltaSharing::Client, @client
    assert_equal @profile, @client.profile
    assert_equal @endpoint, @client.profile['endpoint']
    assert_equal @bearer_token, @client.profile['bearerToken']
  end

  def test_initialize_with_invalid_parameters
    assert_raises(ArgumentError) do
      DeltaSharing::Client.new(endpoint: 'https://example.com/delta-sharing')
    end
    assert_raises(ArgumentError) do
      DeltaSharing::Client.new(bearer_token: 'test-token')
    end
    assert_raises(ArgumentError) do
      DeltaSharing::Client.new(@profile_path, endpoint: 'https://example.com/delta-sharing', bearer_token: 'test-token')
    end
    assert_raises(ArgumentError) do
      DeltaSharing::Client.new(@profile_path, endpoint: 'https://example.com/delta-sharing')
    end
    assert_raises(ArgumentError) do
      DeltaSharing::Client.new(@profile_path, bearer_token: 'test-token')
    end
  end

  def test_initialize_with_invalid_profile
    assert_raises(DeltaSharing::Error) do
      DeltaSharing::Client.new('nonexistent.json')
    end
  end

  def test_list_shares
    stub_request(:get, "#{@endpoint}/shares")
      .with(
        headers: {
          'Authorization' => "Bearer #{@bearer_token}",
          'Content-Type' => 'application/json'
        }
      )
      .to_return(
        status: 200,
        body: {
          items: [
            { name: 'share1', id: '1' },
            { name: 'share2', id: '2' }
          ]
        }.to_json
      )

    shares = @client.list_shares
    assert_equal 2, shares.length
    assert_equal 'share1', shares[0][:name]
    assert_equal '1', shares[0][:id]
    assert_equal 'share2', shares[1][:name]
    assert_equal '2', shares[1][:id]
  end

  def test_get_share
    stub_request(:get, "#{@endpoint}/shares/test_share")
      .with(
        headers: {
          'Authorization' => "Bearer #{@bearer_token}",
          'Content-Type' => 'application/json'
        }
      )
      .to_return(
        status: 200,
        body: {
          share: {
            name: 'test_share',
            id: '1'
          }
        }.to_json
      )

    share = @client.get_share('test_share')
    assert_equal 'test_share', share['share']['name']
    assert_equal '1', share['share']['id']
  end

  def test_list_schemas
    share_name = 'test_share'
    stub_request(:get, "#{@endpoint}/shares/#{share_name}/schemas")
      .with(
        headers: {
          'Authorization' => "Bearer #{@bearer_token}",
          'Content-Type' => 'application/json'
        }
      )
      .to_return(
        status: 200,
        body: {
          items: [
            { name: 'schema1', share: share_name },
            { name: 'schema2', share: share_name }
          ]
        }.to_json
      )

    schemas = @client.list_schemas(share_name)
    assert_equal 2, schemas.length
    assert_equal 'schema1', schemas[0][:name]
    assert_equal share_name, schemas[0][:share]
    assert_equal 'schema2', schemas[1][:name]
    assert_equal share_name, schemas[1][:share]
  end

  def test_list_tables
    share_name = 'test_share'
    schema_name = 'test_schema'
    stub_request(:get, "#{@endpoint}/shares/#{share_name}/schemas/#{schema_name}/tables")
      .with(
        headers: {
          'Authorization' => "Bearer #{@bearer_token}",
          'Content-Type' => 'application/json'
        }
      )
      .to_return(
        status: 200,
        body: {
          items: [
            {
              name: 'table1',
              share: share_name,
              schema: schema_name,
              shareId: '1',
              id: '1'
            }
          ]
        }.to_json
      )

    tables = @client.list_tables(share_name, schema_name)
    assert_equal 1, tables.length
    assert_equal 'table1', tables[0][:name]
    assert_equal share_name, tables[0][:share]
    assert_equal schema_name, tables[0][:schema]
    assert_equal '1', tables[0][:share_id]
    assert_equal '1', tables[0][:id]
  end

  def test_get_table_version
    share_name = 'test_share'
    schema_name = 'test_schema'
    table_name = 'test_table'
    stub_request(:get, "#{@endpoint}/shares/#{share_name}/schemas/#{schema_name}/tables/#{table_name}/version")
      .with(
        headers: {
          'Authorization' => "Bearer #{@bearer_token}",
          'Content-Type' => 'application/json'
        }
      )
      .to_return(
        status: 200,
        body: {}.to_json,
        headers: {
          'delta-table-version' => '1'
        }
      )

    version = @client.get_table_version(share_name, schema_name, table_name)
    assert_equal '1', version
  end

  def test_get_table_metadata
    share_name = 'test_share'
    schema_name = 'test_schema'
    table_name = 'test_table'
    protocol_json = {
      'protocol' => {
        'minReaderVersion' => 1
      }
    }
    metadata_json = {
      'metaData' => {
        'partitionColumns' => [
          'date'
        ],
        'format' => {
          'provider' => 'parquet'
        },
        'schemaString' => '{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}',
        'id' => '1',
        'configuration' => {
          'enableChangeDataFeed' => 'true'
        },
        'size' => 123,
        'numFiles' => 5
      }
    }
    stub_request(:get, "#{@endpoint}/shares/#{share_name}/schemas/#{schema_name}/tables/#{table_name}/metadata")
      .with(
        headers: {
          'Authorization' => "Bearer #{@bearer_token}",
          'Content-Type' => 'application/json'
        }
      )
      .to_return(
        status: 200,
        body: [protocol_json.to_json, metadata_json.to_json].join("\n")
      )

    response = @client.get_table_metadata(share_name, schema_name, table_name)
    assert_equal metadata_json['metaData']['partitionColumns'], response['metaData']['partitionColumns']
    assert_equal metadata_json['metaData']['format']['provider'], response['metaData']['format']['provider']
    assert_equal metadata_json['metaData']['schemaString'], response['metaData']['schemaString']
    assert_equal metadata_json['metaData']['id'], response['metaData']['id']
    assert_equal metadata_json['metaData']['configuration']['enableChangeDataFeed'],
                 response['metaData']['configuration']['enableChangeDataFeed']
    assert_equal metadata_json['metaData']['size'], response['metaData']['size']
    assert_equal metadata_json['metaData']['numFiles'], response['metaData']['numFiles']
  end

  def test_read_table_data
    share_name = 'test_share'
    schema_name = 'test_schema'
    table_name = 'test_table'

    # Build response according to protocol - with proper wrapper objects
    table_data_response = [
      '{"protocol":{"minReaderVersion":1}}',
      '{"metaData":{"partitionColumns":["date"],"format":{"provider":"parquet"},"schemaString":"{\\"type\\":\\"struct\\"}","id":"1","size":123,"numFiles":5}}',
      '{"file":{"url":"file1.parquet","id":"1","size":123,"partitionValues":{"date":"2021-01-01"}}}',
      '{"file":{"url":"file2.parquet","id":"2","size":456,"partitionValues":{"date":"2021-01-02"}}}'
    ].join("\n")

    stub_request(:post, "#{@endpoint}/shares/#{share_name}/schemas/#{schema_name}/tables/#{table_name}/query")
      .with(
        headers: {
          'Authorization' => "Bearer #{@bearer_token}",
          'Content-Type' => 'application/json',
          'delta-sharing-capabilities' => 'responseformat=parquet'
        }
      )
      .to_return(
        status: 200,
        body: table_data_response,
        headers: {
          'Content-Type' => 'application/x-ndjson; charset=utf-8'
        }
      )

    result = @client.read_table_data(share_name, schema_name, table_name)

    # Your client returns an array of JSON strings
    assert_equal 4, result.length

    # Test each line by parsing the JSON
    protocol_line = JSON.parse(result[0])
    assert_equal 1, protocol_line['protocol']['minReaderVersion']

    metadata_line = JSON.parse(result[1])
    assert_equal ['date'], metadata_line['metaData']['partitionColumns']
    assert_equal '1', metadata_line['metaData']['id']

    file1_line = JSON.parse(result[2])
    assert_equal 'file1.parquet', file1_line['file']['url']
    assert_equal '1', file1_line['file']['id']
    assert_equal 123, file1_line['file']['size']
    assert_equal '2021-01-01', file1_line['file']['partitionValues']['date']

    file2_line = JSON.parse(result[3])
    assert_equal 'file2.parquet', file2_line['file']['url']
    assert_equal '2', file2_line['file']['id']
    assert_equal 456, file2_line['file']['size']
    assert_equal '2021-01-02', file2_line['file']['partitionValues']['date']
  end
end
