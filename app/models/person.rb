class Person < ActiveRecord::Base

  # acts_as_taggable # Alias for acts_as_taggable_on :tags
  # acts_as_taggable_on :skills, :interests
  attr_accessible :email, :key, :value

  has_many :people_datas
  has_many :taggings, foreign_key: 'taggable_id'
  has_many :tags, through: :taggings

end