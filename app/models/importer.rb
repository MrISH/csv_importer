class Tag < ActiveRecord::Base
  validates :name, uniqueness: :true
  attr_accessible :name
end
class Tagging < ActiveRecord::Base
  attr_accessible :tag_id, :taggable_id, :taggable_type, :context
end

module Importer
  class Csv
    require "csv"

    def self.parse_csv(csv_file_stream)
      csv_file = csv_file_stream.read
      csv_rows = CSV.parse(csv_file)
      csv_headers_to_return = csv_file.split("\n").first.split(',')

      errors = []
      created_count = 0
      updated_count = 0
      start_time = Time.now

      if csv_file.blank?
        errors << 'You have not selected a file.'
        return errors, created_count, updated_count, csv_rows.size, csv_headers_to_return, (Time.now - start_time)
      end

      if csv_headers_to_return.first != 'email'
        errors << 'The first line must be field headers, not data.'
        return errors, created_count, updated_count, csv_rows.size, csv_headers_to_return, (Time.now - start_time)
      end

      people = []
      csv_rows.dup.each do |csv_row|
        next if csv_row.first == 'email'
        csv_headers = csv_headers_to_return.dup

        if csv_row.size > csv_headers.size
          errors << "Row: #{csv_row} contained too many fields for the number of headers present."
          next
        end

        email_index = csv_headers.find_index { |x| x == 'email' }
        person_field = csv_row[email_index]

        if !(person = Person.find_by_email(person_field)).blank?
         updated_count += 1
        else
          person = Person.new(email: person_field)
          people << person
          created_count += 1
        end
      end
      Person.import people unless people.blank?


      @tag_columns = [:name]
      @tag_values = []
      ActiveRecord::Base.transaction do
        csv_rows.dup.each do |csv_row|
          next if csv_row.first == 'email'
          csv_headers = csv_headers_to_return.dup
          tag_index = csv_headers.find_index { |x| x == 'tags' }
          if !(tags = csv_row[tag_index].split(',')).blank?
            tags.each do |tag|
              @tag_values.push [tag]
            end
          end
        end

        Tag.import @tag_columns, @tag_values
      end

      @tagging_columns = [:tag_id, :taggable_id, :taggable_type, :context]
      @tagging_values = []
      ActiveRecord::Base.transaction do
        csv_rows.dup.each do |csv_row|
          next if csv_row.first == 'email'
          csv_headers = csv_headers_to_return.dup
          tag_index = csv_headers.find_index { |x| x == 'tags' }
          email_index = csv_headers.find_index { |x| x == 'email' }
          person_field = csv_row[email_index]
          person = Person.find_by_email(person_field)

          if !(tags = csv_row[tag_index]).blank?
            tags.split(',').each do |tag|
              Rails.logger.info tag
              tag = Tag.find_by_name(tag)
              @tagging_values.push [tag.id, person.id, 'Person', 'tags']
            end
          end
        end

        Tagging.import @tagging_columns, @tagging_values
      end

      people = Person.includes(:people_datas, :tags, :taggings)
      people_data = []
      @people_data_columns = [:person_id, :key, :value]
      @people_data_values = []
      ActiveRecord::Base.transaction do
        csv_rows.dup.each do |csv_row|
          next if csv_row.first == 'email'
          csv_headers = csv_headers_to_return.dup
          email_index = csv_headers.find_index { |x| x == 'email' }
          person_field = csv_row[email_index]
          person = people.select { |p| p.email == person_field }.first

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
              @people_data_values.push [person.id, csv_headers[index], field]
            end
          end
        end
        PeopleData.import @people_data_columns, @people_data_values
      end

      errors = ['No errors, YAY!'] if errors.blank?
      end_time = Time.now

      return errors, created_count, updated_count, csv_rows.size, csv_headers_to_return, (end_time - start_time)
    end

  end
end
