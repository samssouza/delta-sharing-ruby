# frozen_string_literal: true

module DeltaSharing
  class Schema
    attr_reader :fields, :arrow_schema

    def initialize(schema_string)
      schema_json = JSON.parse(schema_string)
      @fields = schema_json['fields'] || []
      @arrow_schema = build_arrow_schema
    end

    def field_names
      @fields.map { |f| f['name'] }
    end

    def field_index(name)
      @fields.find_index { |f| f['name'] == name }
    end

    def build_arrow_schema
      fields = @fields.map do |field|
        arrow_type = delta_type_to_arrow_type(field['type'])
        Arrow::Field.new(field['name'], arrow_type, field['nullable'])
      end

      Arrow::Schema.new(fields)
    end

    def delta_type_to_arrow_type(delta_type)
      case delta_type
      when 'boolean'
        Arrow::BooleanDataType.new
      when 'byte'
        Arrow::Int8DataType.new
      when 'short'
        Arrow::Int16DataType.new
      when 'integer'
        Arrow::Int32DataType.new
      when 'long'
        Arrow::Int64DataType.new
      when 'float'
        Arrow::FloatDataType.new
      when 'double'
        Arrow::DoubleDataType.new
      when 'string'
        Arrow::StringDataType.new
      when 'binary'
        Arrow::BinaryDataType.new
      when 'date'
        Arrow::Date32DataType.new
      when 'timestamp'
        Arrow::TimestampDataType.new(:micro)
      else
        if delta_type.is_a?(Hash)
          case delta_type['type']
          when 'decimal'
            Arrow::DecimalDataType.new(delta_type['precision'], delta_type['scale'])
          when 'array'
            element_type = delta_type_to_arrow_type(delta_type['elementType'])
            Arrow::ListDataType.new(element_type)
          when 'map'
            key_type = delta_type_to_arrow_type(delta_type['keyType'])
            value_type = delta_type_to_arrow_type(delta_type['valueType'])
            Arrow::MapDataType.new(key_type, value_type)
          when 'struct'
            fields = delta_type['fields'].map do |field|
              field_type = delta_type_to_arrow_type(field['type'])
              Arrow::Field.new(field['name'], field_type, field['nullable'])
            end
            Arrow::StructDataType.new(fields)
          else
            Arrow::StringDataType.new # Fallback to string
          end
        else
          Arrow::StringDataType.new # Fallback to string
        end
      end
    end
  end
end
