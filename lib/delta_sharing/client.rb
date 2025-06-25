# frozen_string_literal: true

module DeltaSharing
  class Client
    attr_reader :profile

    # Initialize with profile file path OR direct credentials
    def initialize(profile_file = nil, endpoint: nil, bearer_token: nil)
      if profile_file && (endpoint || bearer_token)
        raise ArgumentError,
              'Must provide either profile_file path OR both endpoint and bearer_token not both'
      end

      if profile_file.nil? && (endpoint && bearer_token.nil? || endpoint.nil? && bearer_token)
        raise ArgumentError,
              'Must provide both endpoint and bearer_token'
      end

      if profile_file
        @profile = load_profile_from_file(profile_file)
      elsif endpoint && bearer_token
        @profile = {
          'endpoint' => endpoint,
          'bearerToken' => bearer_token
        }
      end
    end

    # List all shares
    def list_shares
      path = '/shares'
      response = make_request(path)
      shares, next_page_token = parse_shares_response(response.body)
      while next_page_token
        response = make_request(path, params: { nextPageToken: next_page_token })
        shares, next_page_token = parse_shares_response(response.body)
      end
      shares
    end

    # Get share info
    def get_share(share_name)
      path = "/shares/#{share_name}"
      response = make_request(path)
      JSON.parse(response.body)
    end

    # List all schemas in a share
    def list_schemas(share_name)
      path = "/shares/#{share_name}/schemas"
      response = make_request(path)
      schemas, next_page_token = parse_schemas_response(response.body)
      while next_page_token
        response = make_request(path, params: { nextPageToken: next_page_token })
        schemas, next_page_token = parse_schemas_response(response.body)
      end
      schemas
    end

    # List all tables in a share and schema
    def list_tables(share_name, schema_name = nil)
      path = if schema_name
               # List tables in a specific schema
               "/shares/#{share_name}/schemas/#{schema_name}/tables"
             else
               # List all tables in the share (across all schemas)
               "/shares/#{share_name}/all-tables"
             end

      response = make_request(path)
      tables, next_page_token = parse_tables_response(response.body)
      while next_page_token
        response = make_request(path, params: { nextPageToken: next_page_token })
        tables, next_page_token = parse_tables_response(response.body)
      end
      tables
    end

    # Query table version (HEAD request)
    def get_table_version(share_name, schema_name, table_name)
      path = "/shares/#{share_name}/schemas/#{schema_name}/tables/#{table_name}/version"
      response = make_request(path, method: 'GET')
      response.headers['delta-table-version']
    end

    # Get table metadata
    def get_table_metadata(share_name, schema_name, table_name)
      path = "/shares/#{share_name}/schemas/#{schema_name}/tables/#{table_name}/metadata"
      response = make_request(path)
      parse_metadata_response(response.body)
    end

    # Read table data using POST request
    def read_table_data(share_name, schema_name, table_name, options = {})
      path = "/shares/#{share_name}/schemas/#{schema_name}/tables/#{table_name}/query"
      body = build_query_body(options)
      response = make_request(path, method: 'POST', body: body)
      parse_newline_delimited_json(response.body)
    end

    # Read table changes using POST request
    def read_table_changes(share_name, schema_name, table_name, options = {})
      path = "/shares/#{share_name}/schemas/#{schema_name}/tables/#{table_name}/changes"
      params = build_changes_params(options)
      response = make_request(path, params: params)
      parse_newline_delimited_json(response.body)
    end

    private

    def load_profile_from_file(profile_file)
      raise Error, "Profile file not found: #{profile_file}" unless File.exist?(profile_file)

      content = File.read(profile_file)
      JSON.parse(content)
    rescue JSON::ParserError => e
      raise Error, "Invalid JSON in profile file: #{e.message}"
    end

    def make_request(path, method: 'GET', params: {}, body: nil)
      url = @profile['endpoint'] + path

      options = {
        headers: {
          'Authorization' => "Bearer #{@profile['bearerToken']}",
          'Content-Type' => 'application/json',
          'delta-sharing-capabilities' => 'responseformat=parquet'
        },
        timeout: 300
      }

      # Add query parameters if provided
      unless params.empty?
        # Filter out nil values and convert to strings
        filtered_params = params.compact.transform_values(&:to_s)
        options[:query] = filtered_params
      end

      # Add body for POST requests
      options[:body] = body.to_json unless body.nil?

      response = case method.upcase
                 when 'GET'
                   HTTParty.get(url, options)
                 when 'HEAD'
                   HTTParty.head(url, options)
                 when 'POST'
                   HTTParty.post(url, options)
                 else
                   raise ArgumentError, "Unsupported HTTP method: #{method}"
                 end

      handle_error_response(response, path) unless response.success?

      response
    end

    def build_query_body(options)
      body = {}

      body[:predicateHints] = options[:predicate_hints] if options[:predicate_hints]
      if options[:json_predicate_hints]
        body[:jsonPredicateHints] = if options[:json_predicate_hints].is_a?(Hash)
                                      JSON.dump(options[:json_predicate_hints])
                                    else
                                      options[:json_predicate_hints]
                                    end
      end
      body[:limitHint] = options[:limit] if options[:limit]
      body[:version] = options[:version] if options[:version]

      body
    end

    def build_changes_params(options)
      params = {}

      params[:startingVersion] = options[:starting_version] if options[:starting_version]
      params[:endingVersion] = options[:ending_version] if options[:ending_version]
      params[:startingTimestamp] = options[:starting_timestamp] if options[:starting_timestamp]
      params[:endingTimestamp] = options[:ending_timestamp] if options[:ending_timestamp]

      params
    end

    def parse_newline_delimited_json(response_body)
      lines = response_body.split("\n")
      lines.reject(&:empty?)
    end

    def handle_error_response(response, path)
      case response.code
      when 401, 403
        raise AuthenticationError, "Authentication failed: #{response.code} #{response.message}"
      when 404
        raise TableNotFoundError, "Resource not found: #{path}"
      when 400
        raise ProtocolError, "Bad request: #{response.body}"
      else
        raise Error, "HTTP request failed: #{path} #{response.code} #{response.message}"
      end
    end

    def parse_tables_response(response)
      response = JSON.parse(response)
      tables = (response['items'] || []).map do |table|
        {
          name: table['name'],
          share: table['share'],
          schema: table['schema'],
          share_id: table['shareId'],
          id: table['id']
        }
      end
      [tables, response['nextPageToken']]
    end

    def parse_shares_response(response)
      response = JSON.parse(response)
      shares = (response['items'] || []).map do |share|
        {
          name: share['name'],
          id: share['id']
        }
      end

      [shares, response['nextPageToken']]
    end

    def parse_schemas_response(response)
      response = JSON.parse(response)
      schemas = (response['items'] || []).map do |schema|
        {
          name: schema['name'],
          share: schema['share']
        }
      end
      [schemas, response['nextPageToken']]
    end

    def parse_metadata_response(response)
      parsed_response = parse_newline_delimited_json(response)
      JSON.parse(parsed_response[1])
    rescue JSON::ParserError => e
      # raise ProtocolError, "Invalid schema JSON in metadata: #{e.message}"
      raise e
    end
  end
end
