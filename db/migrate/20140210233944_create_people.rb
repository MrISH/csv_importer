class CreatePeople < ActiveRecord::Migration
  def change
    create_table :people do |t|
      t.column :email, :string
      t.timestamps
    end

    add_index :people, :email, :unique => true
  end
end
