const url = window.location.href;
var a = document.createElement('a');
a.href = url;
const ip = a.hostname;
console.log(ip)
document.querySelector("iframe").setAttribute("src", "http://".concat(ip, ":", "8501"))

function setURL(port) {
    document.getElementById('streamlit_content').src = "http://".concat(ip, ":", port);
  }

// let textBox = document.body;
// textBox.addEventListener('keydown', (event) => {
//     console.log(`key=${event.key},code=${event.code}`);
// });

function changeArrow(){
  let c = document.querySelector("i").classList[0]
  if (c == 'icon-chevron-up'){
    document.querySelector("i").classList.replace("icon-chevron-up", "icon-chevron-down")
  } else {
    document.querySelector("i").classList.replace("icon-chevron-down", "icon-chevron-up")
  }
}

// function hideShowNavBar(){
//   document.querySelector("#collapse").classList.toggle("hide");
// }
