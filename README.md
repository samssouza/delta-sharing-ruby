# Delta Sharing Ruby Client

A Ruby implementation of the Delta Sharing Protocol for reading shared Delta Lake tables. For now it only suports parquet format. See Todo Bellow

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'delta_sharing'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install delta_sharing

## Usage

### Initialize the Client

```ruby
require 'delta_sharing'

client = DeltaSharing::Client.new('profile.json')
```
or
```ruby
require 'delta_sharing'

client = DeltaSharing::Client.new(endpoint: "https://your-delta-sharing-server/delta-sharing/", bearerToken: "your-bearer-token")
```


### List Shares

```ruby
shares = client.list_shares
```

### List Schemas in a Share

```ruby
schemas = client.list_schemas('share_name')
```

### List Tables

```ruby
# List tables in a specific schema
tables = client.list_tables('share_name', 'schema_name')
```

### Read Table

```ruby
# List tables in a specific schema
client = DeltaSharing::Client.new('profile.json')

# Create a Reader json predicates hints can a string or a hash
# Returns a Arrow:Table
reader = DeltaSharing::Reader.new(table: "#{share_name}.#{schema_name}.#{table_name}", client: client)
aarrow_table = reader.load_as_arrow(limit: 100, json_predicate_hints: '{ "op": "equal", "children": [ { "op": "column", "name": "active", "valueType": "int" }, { "op": "literal", "value": "1", "valueType": "int" } ] }', predicate_hints: ['active = 1'])

```

## Current Implementation Status

🚧 **TODO:**
- Implement table data changes reading functionality
- Add support for reading table as delta format
- Add documentation

## Development
- Fork the project.
- Run bundle
- Make your changes.
- Run bundle exec rake(run tests)
- Crete a PR

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/samssouza/delta-sharing-ruby.
## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Delta Sharing project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/samssouza/delta-sharing-ruby/blob/main/CODE_OF_CONDUCT.md).
