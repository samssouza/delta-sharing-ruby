# frozen_string_literal: true

module DeltaSharing
  class Reader
    attr_reader :table, :client, :share, :schema, :name

    # Table format: <share>.<schema>.<table>
    def initialize(table:, client:, **_options)
      validate_table_format(table)
      @share, @schema, @name = table.split('.')
      @client = client
    end

    # Main reading methods (to be implemented)
    def load_as_arrow(options = {})
      validate_query_options(options)
      read(options)
    end

    private

    def validate_table_format(table)
      # Pattern: anything.anything.anything (where anything = one or more non-dot characters)
      pattern = /\A[^.]+\.[^.]+\.[^.]+\z/

      return if table.is_a?(String) && table.match?(pattern)

      raise ArgumentError, "Invalid table format. Expected '<share>.<schema>.<table>', got: #{table}"
    end

    def validate_query_options(options)
      validate_predicate_hints(options[:predicate_hints]) if options.key?(:predicate_hints)
      validate_json_predicate_hints(options[:json_predicate_hints]) if options.key?(:json_predicate_hints)
      validate_limit(options[:limit]) if options.key?(:limit)
      validate_version(options[:version]) if options.key?(:version)
      validate_timestamp(options[:timestamp]) if options.key?(:timestamp)
    end

    def validate_predicate_hints(hints)
      return if hints.nil?

      raise ArgumentError, "predicate_hints must be an Array, got #{hints.class}" unless hints.is_a?(Array)

      return if hints.all? { |hint| hint.is_a?(String) }

      raise ArgumentError, 'All predicate hints must be strings'
    end

    def validate_json_predicate_hints(hints)
      return if hints.nil?

      unless hints.is_a?(String) || hints.is_a?(Hash)
        raise ArgumentError, "json_predicate_hints must be a String or Hash, got #{hints.class}"
      end

      # If it's a string, try to parse it as JSON to validate
      return unless hints.is_a?(String)

      begin
        JSON.parse(hints)
      rescue JSON::ParserError => e
        raise ArgumentError, "Invalid JSON in predicate hints: #{e.message}"
      end
    end

    def validate_limit(limit_value)
      return if limit_value.nil?

      return if limit_value.is_a?(Integer) && limit_value >= 0

      raise ArgumentError, "limit must be a non-negative Integer, got #{limit_value}"
    end

    def validate_version(version_value)
      return if version_value.nil?

      return if version_value.is_a?(Integer) && version_value >= 0

      raise ArgumentError, "version must be a non-negative Integer, got #{version_value}"
    end

    def validate_timestamp(timestamp_value)
      return if timestamp_value.nil?

      return if timestamp_value.is_a?(String) || timestamp_value.is_a?(Time) || timestamp_value.is_a?(Integer)

      raise ArgumentError, "timestamp must be a String, Time, or Integer, got #{timestamp_value.class}"
    end

    # Read change data feed
    def changes(starting_version: nil, ending_version: nil, starting_timestamp: nil, ending_timestamp: nil)
      response = client.read_table_changes(share, schema, name, {
                                             starting_version: starting_version,
                                             ending_version: ending_version,
                                             starting_timestamp: starting_timestamp,
                                             ending_timestamp: ending_timestamp
                                           })

      process_read_response(response)
    end

    # Read table data with optional filtering
    def read(limit: nil, predicate_hints: nil, json_predicate_hints: nil, version: nil)
      response = client.read_table_data(share, schema, name, {
                                          limit: limit,
                                          predicate_hints: predicate_hints,
                                          json_predicate_hints: json_predicate_hints,
                                          version: version
                                        })

      process_read_response(response)
    end

    def process_read_response(response_lines)
      metadata_line = nil
      file_lines = []

      # Parse newline-delimited JSON response
      response_lines.each do |line|
        line = line.strip
        next if line.empty?

        begin
          json_obj = JSON.parse(line)

          if json_obj['protocol']
            next
          elsif json_obj['metaData']
            metadata_line = json_obj
          elsif json_obj['file']
            file_lines << json_obj
          end
        rescue JSON::ParserError => e
          raise ProtocolError, "Invalid JSON in response line: #{e.message}"
        end
      end
      @files = file_lines
      @metadata = metadata_line
      arrow_schema = Schema.new(@metadata['metaData']['schemaString']).arrow_schema
      # Download and process Parquet files

      arrow_tables = []
      file_lines.each do |file_obj|
        file_info = file_obj['file']
        arrow_table = download_and_read_parquet(file_info)
        arrow_table = add_partition_columns(arrow_table, arrow_schema, file_info) if file_info['partitionValues']
        arrow_tables << arrow_table if arrow_table
      end

      # Combine all Arrow tables
      @arrow_table = if arrow_tables.length == 1
                       arrow_tables.first
                     else
                       combine_arrow_tables(arrow_tables)
                     end

      @arrow_table
    end

    def download_and_read_parquet(file_info)
      url = file_info['url']

      # Download Parquet file
      parquet_data = download_file(url)

      # Read with Apache Arrow
      read_parquet_data(parquet_data)
    end

    def download_file(url)
      response = HTTParty.get(url)

      unless response.success?
        raise NetworkError,
              "Failed to download file from #{url}: #{response.code} #{response.message}"
      end

      response.body
    end

    def read_parquet_data(data)
      # Create a temporary file to write the Parquet data
      Tempfile.create(['delta_sharing', '.parquet']) do |temp_file|
        temp_file.binmode
        temp_file.write(data)
        temp_file.rewind

        # Read using Apache Arrow
        Arrow::Table.load(temp_file.path, format: :parquet)
      end
    rescue StandardError => e
      raise ProtocolError, "Failed to read Parquet data: #{e.message}"
    end

    def add_partition_columns(arrow_table, schema, file_info)
      partition_values = file_info['partitionValues'] || {}
      return arrow_table if partition_values.empty?

      partition_fields = []
      partition_column_values = []

      schema.fields.each do |field|
        if partition_values.keys.include?(field.name)
          partition_fields << field
          partition_column_values << partition_values[field.name]
        end
      end

      return arrow_table if partition_fields.empty?

      # Create row-oriented data: each row is an array of partition values
      partition_rows = Array.new(arrow_table.n_rows) { partition_column_values.dup }

      partition_schema = Arrow::Schema.new(partition_fields)
      partition_table = Arrow::Table.new(partition_schema, partition_rows)
      arrow_table.merge(partition_table)
    end

    def combine_arrow_tables(tables)
      return tables.first if tables.length == 1

      # Use the first table's schema as the reference for column order
      reference_schema = tables.first.schema
      reference_column_order = reference_schema.fields.map(&:name)

      # Align all tables to match the reference schema
      aligned_tables = tables.map do |table|
        current_column_order = table.schema.fields.map(&:name)

        # Check if reordering is needed
        if current_column_order == reference_column_order
          table # Already in correct order
        else
          # Verify all required columns are present
          missing_columns = reference_column_order - current_column_order
          extra_columns = current_column_order - reference_column_order

          raise ProtocolError, "Table missing required columns: #{missing_columns.join(', ')}" if missing_columns.any?

          if extra_columns.any?
            # Log warning but continue (extra columns will be ignored)
            puts "Warning: Table has extra columns that will be ignored: #{extra_columns.join(', ')}"
          end

          # Reorder columns to match reference schema
          reordered_columns = reference_column_order.map do |column_name|
            table.column(column_name)
          end

          Arrow::Table.new(reference_schema, reordered_columns)
        end
      end

      # Now combine all aligned tables
      record_batches = aligned_tables.flat_map { |t| t.each_record_batch.to_a }
      Arrow::Table.new(reference_schema, record_batches)
    rescue StandardError => e
      raise ProtocolError, "Failed to combine Arrow tables: #{e.message}"
    end
  end
end
