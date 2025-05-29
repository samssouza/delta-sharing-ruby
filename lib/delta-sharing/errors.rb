# frozen_string_literal: true

module DeltaSharing
  class Error < StandardError; end
  class AuthenticationError < Error; end
  class TableNotFoundError < Error; end
  class ProtocolError < Error; end
  class NetworkError < Error; end
  class SchemaError < Error; end
  class ParsingError < Error; end
end
