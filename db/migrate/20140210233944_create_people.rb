class CreatePeople < ActiveRecord::Migration
  def change
    create_table :people do |t|
      t.column :email, :string
      t.timestamps
    end
  end
end
