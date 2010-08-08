module OMRF
module TimeDecorator
  def log(type,msg)
    super(type,"#{Time.now.utc}: #{msg}")
  end
end
end