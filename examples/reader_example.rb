#!/usr/bin/env ruby
# frozen_string_literal: true

require 'bundler/setup'
require 'delta-sharing'

# Example of reading a Delta Sharing table with Apache Arrow

# Initialize the client with a profile file
profile_path = File.join(__dir__, '..', 'config.share')
client = DeltaSharing::Client.new(profile_path)

# List available shares
share_name = client.list_shares.first[:name]
schema_name = client.list_schemas(share_name).first[:name]
table_name = client.list_tables(share_name, schema_name).first[:name]

# Create a Table object
reader = DeltaSharing::Reader.new(table: "#{share_name}.#{schema_name}.#{table_name}", client: client)

# Read the table data as an Arrow::Table
puts 'Reading table data...'
arrow_table = reader.load_as_arrow(limit: 100,
                                   json_predicate_hints: '{ "op": "equal", "children": [ { "op": "column", "name": "active", "valueType": "int" }, { "op": "literal", "value": "1", "valueType": "int" } ] }', predicate_hints: ['active = 1'])
# Display first few rows
if arrow_table.n_rows > 0
  puts 'First 5 rows:'
  arrow_table.slice(0, [5, arrow_table.n_rows].min).each_record.with_index do |record, i|
    puts "  Row #{i}: #{record.to_h}"
  end
else
  puts 'Table is empty'
end
