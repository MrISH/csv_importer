class CsvImporterController < ApplicationController

  def index
    @errors ||= ''
  end

  def import
    flash[:results] = Importer::Csv.parse_csv(params[:csv_file])
    redirect_to action: 'results'
  end

  def results
    @errors, @created_count, @updated_count, @total_row_count, @headers, @time = flash[:results]
  end

end
