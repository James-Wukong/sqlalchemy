# parse username by the whitespace in name
def parse_name(fullname: str)->tuple():
    fullname = fullname.split()
    print(fullname)
    if len(fullname) == 0:
        return tuple()
    elif len(fullname) == 1:
        return (fullname[0], '', '')
    elif len(fullname) == 2:
        return (fullname[0], '', fullname[1])
    elif len(fullname) == 3:
        return (fullname[0], fullname[1], fullname[2])
    else:
        return (fullname[0], '', '')
    
def unit_price(total, quantity, discount):
    discount = 0.99 if discount == 1 else discount
    return round(total/(quantity*(1-discount)), 2)
