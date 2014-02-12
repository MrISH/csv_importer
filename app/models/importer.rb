module Importer
  class Csv
    CSV_GROUP_SIZE = 1000
    require "csv"

    def self.parse_csv(csv_file_stream)
      csv_file = csv_file_stream.read
      csv_row_groups = CSV.parse(csv_file).in_groups_of(CSV_GROUP_SIZE)
      csv_headers_to_return = csv_file.split("\n").first.split(',')

      errors = []
      created_count = 0
      updated_count = 0
      start_time = Time.now

      if csv_file.blank?
        errors << 'You have not selected a file.'
        return errors, created_count, updated_count, csv_row_groups.size * CSV_GROUP_SIZE, csv_headers_to_return, (Time.now - start_time)
      end

      if csv_headers_to_return.first != 'email'
        errors << 'The first line must be field headers, not data.'
        return errors, created_count, updated_count, csv_row_groups.size * CSV_GROUP_SIZE, csv_headers_to_return, (Time.now - start_time)
      end

      csv_row_groups.each do |csv_rows|
        csv_rows.compact!

        # People
        ActiveRecord::Base.transaction do
          csv_rows.dup.each do |csv_row|
            next if csv_row.first == 'email'
            csv_headers = csv_headers_to_return.dup

            if csv_row.size > csv_headers.size
              errors << "Row: #{csv_row} contained too many fields for the number of headers present."
              next
            end

            email_index = csv_headers.find_index { |x| x == 'email' }
            person_field = csv_row[email_index]

            ActiveRecord::Base.connection.execute (<<-EOS)
              INSERT INTO `people` SET `email`='#{person_field}', `created_at`='#{Time.now.strftime("%Y-%m-%d %I:%M:%S")}', `update_at`='#{Time.now.strftime("%Y-%m-%d %I:%M:%S")}' ON DUPLICATE KEY UPDATE `email`='#{person_field}', `update_at`='#{Time.now.strftime("%Y-%m-%d %I:%M:%S")}'
            EOS
            created_count += 1
          end
        end

        # Tags
        ActiveRecord::Base.transaction do
          csv_rows.dup.each do |csv_row|
            next if csv_row.first == 'email'
            csv_headers = csv_headers_to_return.dup
            tag_index = csv_headers.find_index { |x| x == 'tags' }
            if !(tags = csv_row[tag_index].split(',')).blank?
              tags.each do |tag|
                ActiveRecord::Base.connection.execute (<<-EOS)
                  INSERT INTO `tags` SET `name`='#{tag}' ON DUPLICATE KEY UPDATE  `name`='#{tag}'
                EOS
              end
            end
          end

          # taggings (join table for tags)
          csv_rows.dup.each do |csv_row|
            next if csv_row.first == 'email'
            csv_headers = csv_headers_to_return.dup
            tag_index = csv_headers.find_index { |x| x == 'tags' }
            email_index = csv_headers.find_index { |x| x == 'email' }
            person_field = csv_row[email_index]
            @person = Person.find_by_email(person_field)

            if !(tags = csv_row[tag_index]).blank?
              tags.split(',').each do |tag|
                tag = Tag.find_by_name(tag)
                ActiveRecord::Base.connection.execute (<<-EOS)
                  INSERT INTO `taggings` SET `tag_id`=#{tag.id}, `taggable_id`=#{@person.id rescue 'NULL'}, `taggable_type`='Person', `context`='tags' ON DUPLICATE KEY UPDATE `tag_id`=#{tag.id}
                EOS
              end
            end
          end

          # People Data
          csv_rows.dup.each do |csv_row|
            next if csv_row.first == 'email'
            csv_headers = csv_headers_to_return.dup
            email_index = csv_headers.find_index { |x| x == 'email' }
            person_field = csv_row[email_index]
            @person = Person.find_by_email(person_field)

            if csv_row.size > csv_headers.size
              errors << "Row: #{csv_row} contained too many fields for the number of headers present."
              next
            end
            tag_index = csv_headers.find_index { |x| x == 'tags' }
            csv_headers.delete_at(tag_index)
            csv_row.delete_at(tag_index)
            email_index = csv_headers.find_index { |x| x == 'email' }
            csv_row.delete_at(email_index)
            csv_headers.delete_at(email_index)

            if !csv_row.blank?
              csv_row.each_with_index do |field, index|
                Rails.logger.info csv_headers[index]
                ActiveRecord::Base.connection.execute (<<-EOS)
                  INSERT INTO `people_data` SET `value`='#{field}', `person_id`=#{@person.id rescue 'NULL'}, `key`='#{csv_headers[index]}', `created_at`='#{Time.now.strftime("%Y-%m-%d %I:%M:%S")}', `update_at`='#{Time.now.strftime("%Y-%m-%d %I:%M:%S")}' ON DUPLICATE KEY UPDATE `value`='#{field}', `update_at`='#{Time.now.strftime("%Y-%m-%d %I:%M:%S")}'
                EOS
              end
            end
          end
        end
      end

      errors = ['No errors, YAY!'] if errors.blank?
      end_time = Time.now

      return errors, created_count, updated_count, created_count+updated_count+(errors.size-1), csv_headers_to_return, (end_time - start_time)
    end

  end
end
