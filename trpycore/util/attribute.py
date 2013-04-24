NO_DEFAULT = object()

def xgetattr(obj, name, default=NO_DEFAULT):
    try:
        current = obj
        for attribute in name.split("."):
            if isinstance(current, dict):
                current = current[attribute]
            else:
                current = getattr(current, attribute)
        result = current
    except (AttributeError, KeyError):
        if default is NO_DEFAULT:
            raise
        else:
            result = default
    return result

def xsetattr(obj, name, value):
    names = name.split(".")
    attribute_name = names.pop()

    current = obj
    for attribute in names:
       if isinstance(current, dict):
           current = current[attribute]
       else:
           current = getattr(current, attribute)
    
    if isinstance(current, dict):
        current[attribute_name] = value
    else:        
        setattr(current, attribute_name, value)
    
    return value

