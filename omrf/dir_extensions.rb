class Dir
  def empty?
    Dir.glob("#{ path }/*", File::FNM_DOTMATCH) do |e|
      return false unless %w( . .. .DS_Store ).include?(File::basename(e))
    end
    return true
  end
  def self.empty? path
    new(path).empty?
  end
end
