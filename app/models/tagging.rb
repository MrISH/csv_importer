class Tagging < ActiveRecord::Base
  attr_accessible :tag_id, :taggable_id, :taggable_type, :context
  validates :tag_id, uniqueness: { scope: :taggable_id }
end
