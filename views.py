from django.core.urlresolvers import reverse
from django.http import HttpResponse
from django.shortcuts import render, redirect
from Database import DB


def initialize_database(request):
    database = DB()
    database.initialization()

    # User.objects.initialize()
    # Product.objects.initialize()
    # Department.objects.initialize()

    return redirect('/')


def main(request):
    database = DB()
    msgs = []
    if 'fromPrice' in request.GET and 'toPrice' in request.GET \
            and request.GET['fromPrice'] != '' and request.GET['toPrice'] != '':
        list = database.getSaleListByPrice(int(request.GET['fromPrice']), int(request.GET['toPrice']))
        msgs.append('by price car')
    elif 'idcar' in request.GET and request.GET['idcar'] != '0':
        list = database.getSaleListByCarID(int(request.GET['idcar']))
        msgs.append('by price car')
    elif 'excludeWord' in request.GET and request.GET['excludeWord'] != '':
        list = database.getListExcluded(request.GET['excludeWord'])
        msgs.append('without : ' + request.GET['excludeWord'])
    elif 'includeWord' in request.GET and request.GET['includeWord'] != '':
        list = database.getListIncluded(request.GET['includeWord'])
        msgs.append('with : ' + request.GET['includeWord'])
    else:
        list = database.getSaleFull()
    car = database.getGistList('car')
    return render(request, 'main_page.html', {'msgs': msgs, 'list': list, 'car': car})


def remove(request, id):
    database = DB()
    database.removeSale(id)
    return redirect('/')


def edit(request, id):
    database = DB()
    if request.method == 'GET':
        departmen = database.getGistList('departmen')
        car = database.getGistList('car')
        client = database.getGistList('client')
        sale = database.getGist('sale', id)
        return render(request, 'edit_page.html', {'departmen': departmen, 'car': car, 'client': client, 'sale': sale})
    else:
        database.updateSale(id, request.POST['idcar'], request.POST['idclient'],
                             request.POST['iddepartmen'], request.POST['price'], request.POST['data'])
        return redirect('/')


def add(request):
    database = DB()
    if request.method == 'GET':
        departmen = database.getGistList('departmen')
        car = database.getGistList('car')
        client = database.getGistList('client')
        return render(request, 'add_page.html', {'departmen': departmen, 'car': car, 'client': client})
    elif request.method == 'POST':
        database.insertSale(request.POST['idcar'], request.POST['idclient'], request.POST['iddepartmen'],
                           request.POST['price'], request.POST['data'])
        return redirect('/')
