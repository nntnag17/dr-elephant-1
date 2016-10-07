import DS from 'ember-data';
import Ember from 'ember';

export default DS.JSONAPIAdapter.extend({
  host: 'http://annag-ld1:8090/rest'
  //host: 'http://ltx1-hcl0582.grid.linkedin.com:8080/rest'
  //host: 'http://localhost:8090/rest'
  //namespace: 'rest',
});

//export default DS.RESTAdapter.extend({
//  namespace: 'rest',
//  pathForType: function (type) {
//    return  Ember.String.pluralize(type);
//  }
//});
