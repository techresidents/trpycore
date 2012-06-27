def group(n, min, max):
    """Return grouping coefficients for n items with  min / max group sizes.
    
    Algorithm will produce a solution which favors the maximum number 
    of groups with max items. This may result in a solution which
    also contains a maximum number of groups with min items.

    Arguments:
        n: number of items (integer)
        min: min group size (integer)
        max: max group size (integer)
    
    Returns:
        List of grouping coefficients for [min ... max].
        For example, group(29, 7, 8) will return
        [3, 1] (3*7 + 1*8 == 29).

        Or None if grouping not possible.
    """

    result = None

    if n == 0:
        result = [0 for i in range(min, max +1)]

    elif min > max:
        result = None

    elif n >= min and n <= max:
        result = [0 for i in range(min, max + 1)]
        result[n - min] = 1 
        
    elif n > max:
        for nmax in range(n / max, 0, -1):
            remainder = n - (nmax * max)
            result = group(remainder, min, max -1) 
            if result is not None:
                result.append(nmax)
                break
        else:
            result = group(n, min, max -1)
            if result is not None:
                result.append(0)

    return result                
