

def func(a, lst=[]):
    for i in range(0,a):
        b = 2 * i
        lst.append(b)
    print(lst)

func(2)
func(3, [7,3,8])
func(4)