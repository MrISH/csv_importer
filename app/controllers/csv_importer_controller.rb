class CsvImporterController < ApplicationController

  def index
    @errors ||= ''
  end

  def import
    flash[:results] = Importer::Csv.parse(params[:csv_file])
    redirect_to action: 'results'
  end

  def results
    @errors, @users_parsed_count, @total_row_count, @headers, @time = flash[:results]
  end

end
