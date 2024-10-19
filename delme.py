def test(perv:int):
    a=1
    b=2
    c=3
    d=4
    e=5
    f=6
    g=7
    h=8
    i=9
    j=10
    k=11
    l=12
    m=13
    n=14
    o=15
    p=16
    q=17
    r=18
    s=19
    t=20
    u=21
    a=a+perv
    b=b+perv
    c=c+perv
    d=d+perv
    e=e+perv
    f=f+perv
    g=g+perv
    h=h+perv
    i=i+perv
    j=j+perv
    k=k+perv
    l=l+perv
    m=m+perv
    n=n+perv
    o=o+perv
    p=p+perv
    q=q+perv
    r=r+perv
    s=s+perv
    t=t+perv
    u=u+perv

def ddd():
    test(1)

def area_circle(r:float):
    """
    расчет площади круга
    :param r: радиус
    :return: площадь
    """
    return 3.14*r**2

class Machine:
    """
    класс Машина
    """

    def __init__(self, brand, model, year):
        """
        инициализация машины
        :param brand: бренд
        :param model: модель
        :param year: год выпуска
        """
        self.brand = brand
        self.model = model
        self.year = year

    def __str__(self):
        """
        строковое представление машины
        :return: строка
        """
        return f"{self.brand} {self.model} {self.year} года"

        """
        строковое представление машины
        :return: строка
        """
        return f"{self.brand} {self.model} {self.year} года"

if __name__ == '__main__':
    car1 = Machine('Toyota', 'Camry', 1999)
    car2 = Machine('Ford', 'Mustang', 2000)
    car3 = Machine('Nissan', 'Altima', 2001)
    car4 = Machine('Honda', 'Civic', 2002)
    car5 = Machine('Mazda', '3', 2003)
    car6 = Machine('Subaru', 'Impreza', 2004)
    car7 = Machine('Kia', 'Optima', 2005)
    car8 = Machine('Hyundai', 'Elantra', 2006)
    car9 = Machine('Volkswagen', 'Jetta', 2007)
    car10 = Machine('BMW', '3 series', 2008)