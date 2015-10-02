module S3DB
  class Utils
    class << self
      def sanitize(input)
        raise ArgumentError, 'invalid input!' unless input =~ /^\w+$/i

        input.to_s
      end
    end
  end
end
