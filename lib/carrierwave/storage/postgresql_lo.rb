# encoding: utf-8
module CarrierWave
  module Storage
    class PostgresqlLo < Abstract
      class File
        if defined?(JRUBY_VERSION)
          include CarrierWave::Storage::Adapters::JDBCConnection
        else
          include CarrierWave::Storage::Adapters::PGConnection
        end

        def initialize(uploader)
          @uploader = uploader
        end

        def url
          "/#{@uploader.model.class.name.underscore.gsub('/', '_')}_#{@uploader.mounted_as.to_s.underscore}/#{identifier}"
        end

        def content_type
        end

        alias :size :file_length

        def connection
          @connection ||= @uploader.model.class.connection.raw_connection
        end

        def identifier
          @oid ||= @uploader.identifier.to_i
        end

        def original_filename
          identifier.to_s
        end

      end

      def store!(file)
        raise "This uploader must be mounted in an ActiveRecord model to work" unless uploader.model
        stored = CarrierWave::Storage::PostgresqlLo::File.new(uploader)
        stored.write(file)
        stored
      end

      def retrieve!(identifier)
        raise "This uploader must be mounted in an ActiveRecord model to work" unless uploader.model
        @oid = identifier
        CarrierWave::Storage::PostgresqlLo::File.new(uploader)
      end

      def identifier
        @oid ||= create_large_object
      end

      def connection
        @connection ||= uploader.model.class.connection.raw_connection
      end

      class CacheFile
        CarrierwavePostgresqlCache = Class.new(ActiveRecord::Base)

        def initialize(cache_klass = CarrierwavePostgresqlCache)
          @cache_klass = cache_klass
        end

        def connection
          @connection ||= @cache_klass.connection.raw_connection
        end

        def content_type
        end

        def write(key, file)
          @key = key
          @cache_klass.transaction do
            cpc     = @cache_klass.find_or_initialize_by(key: key)
            cpc.oid ||= connection.lo_creat
            lo      = connection.lo_open(cpc.oid, ::PG::INV_WRITE)
            connection.lo_truncate(lo, 0)
            cpc.size = connection.lo_write(lo, (@content = file.read))
            connection.lo_close(lo)
            cpc.save if cpc.changed? || cpc.new_record?
            cpc.size
          end
        end

        def read(key = nil)
          if key.nil?
            return @content || fail('Hm, #write was not called before?..')
          end
          cpc = @cache_klass.find_by_key(key)
          return unless cpc
          @cache_klass.transaction do
            lo = connection.lo_open(cpc.oid, ::PG::INV_READ)
            content = connection.lo_read(lo, cpc.size)
            connection.lo_close(lo)
            content
          end
        end

        def delete(key = nil)
          if key.nil?
            key = @key || fail('Hm, #write was not called before?..')
          end
          cpc = @cache_klass.find_by_key(key)
          return unless cpc
          @cache_klass.transaction do
            connection.lo_unlink(cpc.oid)
            CarrierwavePostgresqlCache.delete_all(key: key)
          end
        end

        def to_file
          self
        end

        def closed?
          true
        end
      end

      def cache!(new_file)
        cache_id  = uploader.send(:cache_id)
        filename  = uploader.filename
        full_name = uploader.file.file
        key       = full_name.match(%r{(#{cache_id}.*?#{filename})})[1]

        cached = CarrierWave::Storage::PostgresqlLo::CacheFile.new
        cached.write(key, new_file)
        # FIXME black magic! mimic to CarrierWave::Storage::File#cache!
        new_file.move_to(::File.expand_path(uploader.cache_path, uploader.root), uploader.permissions, uploader.directory_permissions, true)
        cached
      end

      def delete_dir!(path)
        if path
          begin
            FileUtils.rm_r(::File.expand_path(path, uploader.root))
          rescue Errno::ENOENT
            # Ignore: path does not exist
          end
        end
      end

      private
      def create_large_object
        if defined?(JRUBY_VERSION)
          connection.connection.getLargeObjectAPI.createLO
        else
          connection.lo_creat
        end
      end
    end
  end
end
