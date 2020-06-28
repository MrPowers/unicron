class CustomTransform:
  def __init__(self, transform, cols_added = [], cols_removed = [], required_cols = []):
    self.transform = transform
    self.cols_added = cols_added
    self.cols_removed = cols_removed
    self.required_cols = required_cols
