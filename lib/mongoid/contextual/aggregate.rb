# encoding: utf-8
module Mongoid
  module Contextual
    class Aggregate
      include Enumerable
      include Command

      delegate :[], to: :results
      delegate :==, :empty?, to: :entries

      # Iterates over each of the documents in the aggregate, excluding the
      # extra information that was passed back from the database.
      #
      # @example Iterate over the results.
      #   aggragate.each do |doc|
      #     p doc
      #   end
      #
      # @return [ Enumerator ] The enumerator.
      def each
        if block_given?
          documents.each do |doc|
            yield doc
          end
        else
          to_enum
        end
      end

      # Initialize the new aggregate directive.
      #
      # @example Initialize the new aggregate.
      #   Aggregate.new(criteria, map, reduce)
      #
      # @param [ Criteria ] criteria The Mongoid criteria.
      # @param [ String ] group The group js string
      # @param [ Symbol ] unwind Document element to unwind
      def initialize(collection, criteria, group, unwind=nil)
        @collection, @criteria = collection, criteria
        command[:aggregate] = collection.name.to_s
        command[:pipeline]  ||= [{"$group" => group}]
        command[:pipeline][0]["$unwind"] = "$#{unwind}" if unwind
        apply_criteria_options
      end

      # Get the raw output from the aggregate operation.
      #
      # @example Get the raw output.
      #   aggregate.raw
      #
      # @return [ Hash ] The raw output.
      def raw
        results
      end

      # Execute the aggregate, returning the raw output.
      # Useful when you don't care about aggregate's ouptut.
      #
      # @example Run the aggregate
      #   aggregate.execute
      #
      # @return [ Hash ] The raw output
      alias :execute :raw

      # Execute the aggregate, returning the results.
      #
      # @example Get the aggregate results
      #   aggregate.all
      #
      # @return [ Array ] aggregate results
      def all
        documents
      end

      # Get a pretty string representation of the aggregate, including the
      # criteria, map, reduce, finalize, and out option.
      #
      # @example Inspect the aggregate.
      #   aggregate.inspect
      #
      # @return [ String ] The inspection string.
      #
      # @since 3.1.0
      def inspect
        ::I18n.translate(
          "mongoid.inspection.aggregate",
          {
            match:   criteria.selector.inspect,
            options: criteria.options.inspect,
            klass:   criteria.klass.inspect,
            group:   command[:pipeline][0]["$group"],
            unwind:  command[:pipeline][0]["$unwind"]
          }
        )
      end

      private

      # Apply criteria specific options - query, sort, limit.
      #
      # @api private
      #
      # @example Apply the criteria options
      #   aggregate.apply_criteria_options
      #
      # @return [ nil ] Nothing.
      #
      # @since 3.0.0
      def apply_criteria_options
        unless criteria.selector.empty?
          command[:pipeline][0]["$match"] = criteria.selector
        end
        if sort = criteria.options[:sort]
          command[:pipeline][0]["$sort"] = sort
        end
        if limit = criteria.options[:limit]
          command[:pipeline][0]["$limit"] = limit
        end
        if skip = criteria.options[:skip]
          command[:pipeline][0]["$skip"] = skip
        end
        if fields = criteria.options[:fields]
          command[:pipeline][0]["$project"] = fields
        end
      end

      # Get the result documents from the aggregate.
      #
      # @api private
      #
      # @example Get the documents.
      #   aggregate.documents
      #
      # @return [ Array, Cursor ] The documents.
      def documents
        return results["result"] if results.has_key?("result")
      end

      # Execute the aggregate command and get the results.
      #
      #@api private
      #
      # @example Get the results.
      #   aggregate.results
      #
      # @return [ Hash ] The results of the command.
      def results
        @results ||= session.with(consistency: :strong).command(command)
      end
    end
  end
end
