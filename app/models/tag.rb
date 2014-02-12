class Tag < ActiveRecord::Base
  validates :name, uniqueness: :true
  attr_accessible :name
end
