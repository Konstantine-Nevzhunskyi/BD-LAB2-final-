from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^(?P<id>[0-9]+)/edit/$', views.edit, name='edit_page'),
    url(r'^add/$', views.add, name='add_page'),
    url(r'^init/$', views.initialize_database, name='init'),
    url(r'^remove/(?P<id>[0-9]+)/$', views.remove),
    url(r'^$', views.main, name='main_page'),
]

