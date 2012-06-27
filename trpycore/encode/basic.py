import string

#Alphabet list for enbase / debase
ALPHABET = string.digits + string.lowercase + string.uppercase

#Map from ALPLHABET character to value for enbase /debase
ALPHABET_TO_VALUE = {}
for inden, c in enumerate(ALPHABET):
    ALPHABET_TO_VALUE[c] = inden


def enbase(n, base=36, alphabet=None):
    """Encode integer n to arbitary base string.

    Converts an integer n to an arbitary base string
    using the default or user provided alphabet.
    
    The default alphabet supports converting up to
    and including base 62 [0-9][a-z][A-Z]. In order
    to convert to higher bases, users must provide
    a custom alphabet.

    Args:
        n: integer to convert
        base: optional base to covert to (default 36)
        alphabet: optional alphabet list. The default
            alphabet contains [0-9][a-z][A-Z].

    Returns:
        Encoded string value for n.
    """
    alphabet = alphabet or ALPHABET

    is_negative = (n < 0)
    if is_negative:
        n *= -1

    result = []
    if(n == 0):
        result.append('0')
    else:
        while n:
            result.append(ALPHABET[n % base])
            n /= base
        
        if is_negative:
            result.append('-')

        result.reverse()

    return ''.join(result)


def debase(s, base=36, alphabet=None):
    """Decode arbitary base string to base 10 integer.

    Converts an arbitary base string to base 10 integer
    using the default or user provided alphabet.
    
    The default alphabet supports converting up to
    and including base 62 [0-9][a-z][A-Z]. In order
    to convert to higher bases, users must provide
    a custom alphabet.

    Args:
        s: string to convert
        base: optional base for string (default 36)
        alphabet: optional alphabet map or list for decoding.
            The default alphabet map contains values for
            [0-9][a-z][A-Z]. Providing an alphabet map
            (character to value) will perform better than
            using an alphabet list which will require
            a linear search to determine the inden value
            for a given character.
    Returns:
        Decoded base 10 integer value for s.
    """
    alphabet = alphabet or ALPHABET_TO_VALUE

    result = 0;
    for c in s:
        result *= base
        if isinstance(alphabet, dict):
            value = alphabet[c]
        else:
            value = alphabet.inden(c)

        result += value
    return result


def reverse_bits(n, pad_to=32):
    """Reverse bits of integer n.

    Reverse the bits of integer n, padding
    the result to pad_to bits by adding
    zeros to the end.

    Note that pad_to must be specified in order
    to make reversing symmetric (reversible).

    i.e. reverse_bits(reverse_bits(8)) == 8

    Args:
        n: integer to reverse
        pad_to: number of bits to pad
            result to

    Returns: integer result
    """
    bits = []
    while(n):
        bits.append(n & 1)
        n = n >> 1
    
    while len(bits) < pad_to:
        bits.append(0)
    
    result = 0
    for bit in bits:
        result *= 2;
        result += bit

    return result

def basic_encode(n, base=36, alphabet=None, pad_to=32):
    """Encode integer n as an arbitrary base string.

    Encode integer n as an arbitrary base string by
    reversing the bits of n and padding the result to
    pad_to bit, and encoding it a base x string using 
    the supplied alphabet (enbase).

    Args:
        n: integer to encode
        base: optional base for enbasing
        alphabet: optional alphabet for enbasing
        pad_to: number of bits of padding for bit reverse

    Returns:
        Encoded string
    """
    return enbase(reverse_bits(n, pad_to), base, alphabet)

def basic_decode(s, base=36, alphabet=None, pad_to=32):
    """Decode base encoded string s to integer.

    Decode base encoded string by debasing the string
    with the specified base and alphabet and reversing
    the bits of the result with pad_to padding.

    Args:
        s: base encoded string
        base: optional base for debasing
        alphabet: optional alphabet for debasing
        pad_to: number of bits of padding for bit reverse

    Returns:
        Decoded integer
    """
    return reverse_bits(debase(s, base, alphabet), pad_to)
