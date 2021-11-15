use std::borrow::Borrow;
use crate::publisher::Writer;
use crate::{ResourceId, Session};
//->Writer<'a>
pub fn publish<'a,ResourceId, IntoValue>(session: &Session, rid: &ResourceId, value: IntoValue){
    let mut c= &session.state.as_ref().read().unwrap();//as_ref().into_inner().unwrap();
    let a=c.get_res(&1,true).unwrap().subscribers.len();
    let h=c.get_res(&1,false).unwrap().subscribers.len();
    let g=c.local_resources.get(&1).unwrap().subscribers.len();
    let k= c.local_resources.keys();
    let e=&c.local_resources.get(&1).unwrap().name;
    let f=c.remote_resources.len();
    //session.put(reskey,value)
    println!("{},{},{},{:?},{},{:?}",a,h,g,e,f,k);

}