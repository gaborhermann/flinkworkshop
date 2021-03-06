package hu.sztaki.workshop.flink.utils.data;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class StaticData {

	static final List<Tuple3<String, String, String>> USERS = Arrays.asList(
					new Tuple3<String, String, String>("Raul Padron", "Mexico", "13.02.1971."),
					new Tuple3<String, String, String>("Adalger Imhoff", "Germany", "28.11.2001."),
					new Tuple3<String, String, String>("Alina Kaiser", "Germany", "15.04.1965."),
					new Tuple3<String, String, String>("Aluino Avitia", "Mexico", "07.10.1952."),
					new Tuple3<String, String, String>("Andrea Steiner", "Germany", "14.01.1997."),
					new Tuple3<String, String, String>("Apor Angéla", "Hungary", "15.04.1991."),
					new Tuple3<String, String, String>("Aquilino Olea", "Mexico", "25.12.1967."),
					new Tuple3<String, String, String>("Ariela Monsalve", "Mexico", "06.12.1948."),
					new Tuple3<String, String, String>("Belián Linda", "Hungary", "20.04.1977."),
					new Tuple3<String, String, String>("Ben Piltz", "Germany", "03.04.2005."),
					new Tuple3<String, String, String>("Benedek Katinka", "Hungary", "24.02.1956."),
					new Tuple3<String, String, String>("Benedek Miklós", "Hungary", "14.06.1941."),
					new Tuple3<String, String, String>("Borbély István", "Hungary", "01.02.1988."),
					new Tuple3<String, String, String>("Brissa Jacinto", "Mexico", "12.06.2005."),
					new Tuple3<String, String, String>("Britta Breslau", "Germany", "08.03.1950."),
					new Tuple3<String, String, String>("Bruno Hönigswald", "Germany", "07.11.1962."),
					new Tuple3<String, String, String>("Brúnó Robertina", "Hungary", "22.04.1941."),
					new Tuple3<String, String, String>("Cäcilia Schuerer", "Germany", "15.02.1990."),
					new Tuple3<String, String, String>("Carlos Velazco", "Mexico", "08.11.1964."),
					new Tuple3<String, String, String>("Chiara Sundermann", "Germany", "22.10.1991."),
					new Tuple3<String, String, String>("Chrisanna Fulgencio", "Mexico", "24.05.1942."),
					new Tuple3<String, String, String>("Clareta Luciano", "Mexico", "25.11.1949."),
					new Tuple3<String, String, String>("Conrado Marrufo", "Mexico", "17.06.2002."),
					new Tuple3<String, String, String>("Cristian Limon", "Mexico", "07.04.1963."),
					new Tuple3<String, String, String>("Csanádi Ráhel", "Hungary", "26.04.1981."),
					new Tuple3<String, String, String>("Csikós Gusztáv", "Hungary", "25.02.1968."),
					new Tuple3<String, String, String>("Deák Bandi", "Hungary", "07.07.2000."),
					new Tuple3<String, String, String>("Diósi Adrienn", "Hungary", "21.03.1980."),
					new Tuple3<String, String, String>("Dobos Albert", "Hungary", "22.02.1943."),
					new Tuple3<String, String, String>("Dobos Orsika", "Hungary", "18.06.1968."),
					new Tuple3<String, String, String>("Dominic Schenk", "Germany", "02.01.1972."),
					new Tuple3<String, String, String>("Eckardt Lowenstam", "Germany", "26.10.1982."),
					new Tuple3<String, String, String>("Edmund Helén", "Hungary", "01.07.1971."),
					new Tuple3<String, String, String>("Elbertina Medrano", "Mexico", "10.02.1954."),
					new Tuple3<String, String, String>("Elias Gegenbauer", "Germany", "18.10.1982."),
					new Tuple3<String, String, String>("Elija Otero", "Mexico", "15.12.1942."),
					new Tuple3<String, String, String>("Emilia Madritsch", "Germany", "19.04.1995."),
					new Tuple3<String, String, String>("Emily Carillo", "Mexico", "03.02.1948."),
					new Tuple3<String, String, String>("Emma Gustloff", "Germany", "18.01.1985."),
					new Tuple3<String, String, String>("Enriqueta Del Campo", "Mexico", "15.09.1983."),
					new Tuple3<String, String, String>("Erasmo Fraire", "Mexico", "15.05.1956."),
					new Tuple3<String, String, String>("Etele Csilla", "Hungary", "02.11.1940."),
					new Tuple3<String, String, String>("Evelyn Eichenberg", "Germany", "15.01.2007."),
					new Tuple3<String, String, String>("Franco Montes de Oca", "Mexico", "14.04.1956."),
					new Tuple3<String, String, String>("Gábriel Csongor", "Hungary", "07.02.1956."),
					new Tuple3<String, String, String>("Gáspár Sámuel", "Hungary", "18.05.1992."),
					new Tuple3<String, String, String>("Gertrudis Mare", "Mexico", "27.02.1995."),
					new Tuple3<String, String, String>("Géza Jeno", "Hungary", "15.10.1971."),
					new Tuple3<String, String, String>("Gracia Imperial", "Mexico", "28.02.1955."),
					new Tuple3<String, String, String>("Hanno Griebel", "Germany", "13.10.1980."),
					new Tuple3<String, String, String>("Heiner Lauterbach", "Germany", "02.05.2007."),
					new Tuple3<String, String, String>("Huba Milán", "Hungary", "12.01.1981."),
					new Tuple3<String, String, String>("Jákob Gréta", "Hungary", "28.07.1975."),
					new Tuple3<String, String, String>("Jeno Tilda", "Hungary", "02.08.1972."),
					new Tuple3<String, String, String>("Joaquin Benitez", "Mexico", "12.03.2001."),
					new Tuple3<String, String, String>("Judith Vina", "Mexico", "13.09.2001."),
					new Tuple3<String, String, String>("Julian Perales", "Mexico", "25.01.1986."),
					new Tuple3<String, String, String>("Kádár Angéla", "Hungary", "07.01.1980."),
					new Tuple3<String, String, String>("Kai Fein", "Germany", "22.12.1999."),
					new Tuple3<String, String, String>("Kathrin Schöner", "Germany", "07.05.1970."),
					new Tuple3<String, String, String>("Kende Tekla", "Hungary", "06.07.1960."),
					new Tuple3<String, String, String>("Kevin Handmann", "Germany", "03.05.1945."),
					new Tuple3<String, String, String>("Kincses Renáta", "Hungary", "20.02.1976."),
					new Tuple3<String, String, String>("Konrad Lichtenberger", "Germany", "02.10.2006."),
					new Tuple3<String, String, String>("LaCienega Villarreal", "Mexico", "01.08.2004."),
					new Tuple3<String, String, String>("Lena Heidemann", "Germany", "10.07.1949."),
					new Tuple3<String, String, String>("Lisa Grabner", "Germany", "09.09.2004."),
					new Tuple3<String, String, String>("Lorinc Vince", "Hungary", "27.06.1980."),
					new Tuple3<String, String, String>("Lothar Stock", "Germany", "18.04.1979."),
					new Tuple3<String, String, String>("Lovász Vencel", "Hungary", "15.04.1994."),
					new Tuple3<String, String, String>("Manuel Ribbeck", "Germany", "09.05.1992."),
					new Tuple3<String, String, String>("Maren Scheid", "Germany", "20.01.1998."),
					new Tuple3<String, String, String>("Margarita Armenteros", "Mexico", "06.07.1972."),
					new Tuple3<String, String, String>("Maricel Sarmiento", "Mexico", "22.01.1994."),
					new Tuple3<String, String, String>("Marisela Bedolla", "Mexico", "01.10.1950."),
					new Tuple3<String, String, String>("Martin Fontanilla", "Mexico", "01.05.1970."),
					new Tuple3<String, String, String>("Matthäus Rödl", "Germany", "17.04.1988."),
					new Tuple3<String, String, String>("Mayer Frigyes", "Hungary", "17.04.1992."),
					new Tuple3<String, String, String>("Melissa Bolender", "Germany", "10.08.1950."),
					new Tuple3<String, String, String>("Németh Sarolta", "Hungary", "16.05.1997."),
					new Tuple3<String, String, String>("Orlando Marzo", "Mexico", "08.06.1995."),
					new Tuple3<String, String, String>("Oro Ramirez", "Mexico", "16.11.1958."),
					new Tuple3<String, String, String>("Oszkár Amanda", "Hungary", "26.03.1953."),
					new Tuple3<String, String, String>("Özséb Linda", "Hungary", "28.07.1945."),
					new Tuple3<String, String, String>("Paciencia Villafranca", "Mexico", "26.09.1947."),
					new Tuple3<String, String, String>("Papp Jónás", "Hungary", "25.09.1946."),
					new Tuple3<String, String, String>("Péter Ilona", "Hungary", "17.03.2005."),
					new Tuple3<String, String, String>("Pongrác Armand", "Hungary", "15.06.1979."),
					new Tuple3<String, String, String>("Pueblo Alcazar", "Mexico", "10.01.2007."),
					new Tuple3<String, String, String>("Rácz Olivér", "Hungary", "25.08.2005."),
					new Tuple3<String, String, String>("Ralf Klaproth", "Germany", "21.01.1969."),
					new Tuple3<String, String, String>("Ralf Scheerbart", "Germany", "24.04.1942."),
					new Tuple3<String, String, String>("Ramiro Blasco", "Mexico", "12.09.1988."),
					new Tuple3<String, String, String>("Regina Moreno", "Mexico", "19.04.2002."),
					new Tuple3<String, String, String>("Róbert Lorinc", "Hungary", "11.12.1979."),
					new Tuple3<String, String, String>("Roldan Gurule", "Mexico", "27.06.1970."),
					new Tuple3<String, String, String>("Rosario Reynoso", "Mexico", "06.09.1943."),
					new Tuple3<String, String, String>("Sabrina Erlach", "Germany", "19.03.1959."),
					new Tuple3<String, String, String>("Senona Pantano", "Mexico", "23.04.1955."),
					new Tuple3<String, String, String>("Sipos Róbert", "Hungary", "11.04.1987."),
					new Tuple3<String, String, String>("Sissi Thiel", "Germany", "10.08.2006."),
					new Tuple3<String, String, String>("Solymar Artiga", "Mexico", "13.12.1940."),
					new Tuple3<String, String, String>("Somogyi Kristóf", "Hungary", "17.08.2001."),
					new Tuple3<String, String, String>("Soós Orsolya", "Hungary", "09.11.1971."),
					new Tuple3<String, String, String>("Steffen Marx", "Germany", "11.07.1967."),
					new Tuple3<String, String, String>("Stephanie Pauli", "Germany", "09.04.1978."),
					new Tuple3<String, String, String>("Szabolcs Imola", "Hungary", "12.12.1959."),
					new Tuple3<String, String, String>("Szalai Ágota", "Hungary", "03.10.1960."),
					new Tuple3<String, String, String>("Szucs Olívia", "Hungary", "02.05.1966."),
					new Tuple3<String, String, String>("Tamás Arnold", "Hungary", "14.11.1951."),
					new Tuple3<String, String, String>("Tauro Savala", "Mexico", "16.10.1980."),
					new Tuple3<String, String, String>("Tóth Vanda", "Hungary", "10.05.1998."),
					new Tuple3<String, String, String>("Túri Katalin", "Hungary", "23.09.2005."),
					new Tuple3<String, String, String>("Udo Strauss", "Germany", "14.04.1982."),
					new Tuple3<String, String, String>("Váradi Dóra", "Hungary", "16.09.1981."),
					new Tuple3<String, String, String>("Virág ILona", "Hungary", "16.10.1984."),
					new Tuple3<String, String, String>("Woldemar Faerber", "Germany", "25.01.1950."),
					new Tuple3<String, String, String>("Wolf Kunst", "Germany", "03.12.2006."),
					new Tuple3<String, String, String>("Xiomara Chinchilla", "Mexico", "26.11.2004."),
					new Tuple3<String, String, String>("Yannick Neuhaeuser", "Germany", "14.04.1963."),
					new Tuple3<String, String, String>("Zita Urenda", "Mexico", "23.01.2008."),
					new Tuple3<String, String, String>("Zsigmond Rozália", "Hungary", "16.09.1954."),
					new Tuple3<String, String, String>("Theo Harper", "USA", "17.08.1990."),
					new Tuple3<String, String, String>("Gabriel Gardner", "USA", "01.05.1955."),
					new Tuple3<String, String, String>("Aaron Simpson", "USA", "21.09.1962."),
					new Tuple3<String, String, String>("James Gardner", "USA", "05.01.1996."),
					new Tuple3<String, String, String>("Rhys Kelly", "USA", "23.02.1983."),
					new Tuple3<String, String, String>("Enrique Walter", "USA", "10.05.1969."),
					new Tuple3<String, String, String>("Maria Cook", "USA", "28.05.1982."),
					new Tuple3<String, String, String>("Lara Jackson", "USA", "14.04.2001."),
					new Tuple3<String, String, String>("Sara Fields", "USA", "28.04.2005."),
					new Tuple3<String, String, String>("Brenda Benton", "USA", "17.03.1963."),
					new Tuple3<String, String, String>("Katrina Hester", "USA", "08.03.1955."),
					new Tuple3<String, String, String>("Amelia Mcclain", "USA", "15.06.1942."),
					new Tuple3<String, String, String>("Belen Beach", "USA", "13.08.1994."));

	static final List<Tuple2<String, String>> SONGS = Arrays.asList(
			new Tuple2<String, String>("See You Again", "Wiz Khalifa"),
			new Tuple2<String, String>("Sugar", "Maroon 5"),
			new Tuple2<String, String>("Love Me Like You Do", "Ellie Goulding"),
			new Tuple2<String, String>("Thinking Out Loud", "Ed Sheeran"),
			new Tuple2<String, String>("Four Five Seconds", "Rihanna"),
			new Tuple2<String, String>("Chandelier", "Sia"),
			new Tuple2<String, String>("All About That Bass", "Meghan Trainor"),
			new Tuple2<String, String>("A Thousand Years", "Christina Perri"),
			new Tuple2<String, String>("All Of Me", "John Legend"),
			new Tuple2<String, String>("Stand By Me", "Ben E. King"),
			new Tuple2<String, String>("Lost Stars", "Adam Levine"),
			new Tuple2<String, String>("Elastic Heart", "Sia"),
			new Tuple2<String, String>("Stay With Me", "Sam Smith"),
			new Tuple2<String, String>("Big Girls Cry", "Sia"),
			new Tuple2<String, String>("Let It Go", "Idina Menzel"),
			new Tuple2<String, String>("Take Me To Church", "HOZIER"),
			new Tuple2<String, String>("A Whole New World", "Aladdin"),
			new Tuple2<String, String>("Uptown Funk!", "Mark Ronson"),
			new Tuple2<String, String>("Blank Space", "Taylor Swift"),
			new Tuple2<String, String>("Dear Future Husband", "Meghan Trainor"),
			new Tuple2<String, String>("Fire Meet Gasoline", "Sia"),
			new Tuple2<String, String>("Do You Wanna Build a Snowman?", "Kristen Bell"),
			new Tuple2<String, String>("King", "Years & Years"),
			new Tuple2<String, String>("Lay Me Down", "Sam Smith"),
			new Tuple2<String, String>("The Nights", "Avicii"),
			new Tuple2<String, String>("I'm Not The Only One", "Sam Smith"),
			new Tuple2<String, String>("Someone Like You", "Adele"),
			new Tuple2<String, String>("Rolling In The Deep", "Adele"),
			new Tuple2<String, String>("The Hanging Tree", "Soundtrack Artists"),
			new Tuple2<String, String>("Style", "Taylor Swift"),
			new Tuple2<String, String>("The Scientist", "Coldplay"),
			new Tuple2<String, String>("Earned It", "The Weeknd"),
			new Tuple2<String, String>("Talking Body", "Tove Lo"),
			new Tuple2<String, String>("Chasing Cars", "Snow Patrol"),
			new Tuple2<String, String>("Part Of Your World", "Disney"),
			new Tuple2<String, String>("Cup Song", "Anna Kendrick"),
			new Tuple2<String, String>("(Disney's Frozen) Let It Go", "Idina Menzel"),
			new Tuple2<String, String>("Shake It Off", "Taylor Swift"),
			new Tuple2<String, String>("Bailando", "Enrique Iglesias"),
			new Tuple2<String, String>("Rude", "Magic!"),
			new Tuple2<String, String>("Somebody That I Used To Know", "Gotye"),
			new Tuple2<String, String>("Just The Way You Are", "Bruno Mars"),
			new Tuple2<String, String>("Girl Crush", "Little Big Town"),
			new Tuple2<String, String>("Stressed Out", "Twenty One Pilots"),
			new Tuple2<String, String>("Story Of My Life", "One Direction"),
			new Tuple2<String, String>("El Perdón", "Nicky Jam"),
			new Tuple2<String, String>("I'm yours", "Jason Mraz"),
			new Tuple2<String, String>("Trap Queen", "Fetty Wap"),
			new Tuple2<String, String>("Just Give Me A Reason", "Pink"),
			new Tuple2<String, String>("You & I", "One Direction"),
			new Tuple2<String, String>("Creep", "Radiohead"),
			new Tuple2<String, String>("Eye Of The Tiger", "Survivor"),
			new Tuple2<String, String>("All Of The Stars", "Ed Sheeran"),
			new Tuple2<String, String>("Somewhere Over The Rainbow", "Israel Kamakawiwo'ole"),
			new Tuple2<String, String>("Geronimo", "Sheppard"),
			new Tuple2<String, String>("Masterpiece", "Jessie J"),
			new Tuple2<String, String>("Let It Go", "Demi Lovato"),
			new Tuple2<String, String>("One Last Time", "Ariana Grande"),
			new Tuple2<String, String>("Dekat Di Hati", "Ran"),
			new Tuple2<String, String>("Anaconda", "Nicki Minaj"),
			new Tuple2<String, String>("It's The Hard-knock Life", "Annie"),
			new Tuple2<String, String>("Love Me Harder", "Ariana Grande"),
			new Tuple2<String, String>("Price Tag", "Jessie J"),
			new Tuple2<String, String>("Fly Me To The Moon", "Frank Sinatra"),
			new Tuple2<String, String>("The Lazy Song", "Bruno Mars"),
			new Tuple2<String, String>("Photograph", "Ed Sheeran"),
			new Tuple2<String, String>("Want To Want Me", "Jason DeRulo"),
			new Tuple2<String, String>("Fix You", "Coldplay"),
			new Tuple2<String, String>("Boom Clap", "Charli XCX"),
			new Tuple2<String, String>("My Favorite Things (maria)", "The Sound Of Music"),
			new Tuple2<String, String>("Cups (Pitch Perfect's When I'm Gone)", "Anna Kendrick"),
			new Tuple2<String, String>("Lucky", "Jason Mraz"),
			new Tuple2<String, String>("Cheerleader", "Omi"),
			new Tuple2<String, String>("Uncover", "Zara Larsson"),
			new Tuple2<String, String>("Prologue Into The Woods", "Into the Woods"),
			new Tuple2<String, String>("Tomorrow", "Annie"),
			new Tuple2<String, String>("Honey, I'm Good", "Andy Grammer"),
			new Tuple2<String, String>("Roar", "Katy Perry"),
			new Tuple2<String, String>("Grenade", "Bruno Mars"),
			new Tuple2<String, String>("A Sky Full of Stars", "Coldplay"),
			new Tuple2<String, String>("Set Fire To The Rain", "Adele"),
			new Tuple2<String, String>("Call Me Maybe", "Carly Rae Jepsen"),
			new Tuple2<String, String>("Love Is an Open Door", "Kristen Bell"),
			new Tuple2<String, String>("Marry You", "Bruno Mars"),
			new Tuple2<String, String>("Maps", "Maroon 5"),
			new Tuple2<String, String>("Let Her Go", "Passenger"),
			new Tuple2<String, String>("The Heart Wants What It Wants", "Selena Gomez"),
			new Tuple2<String, String>("Night Changes", "One Direction"),
			new Tuple2<String, String>("Demons", "Imagine Dragons"),
			new Tuple2<String, String>("OctaHate", "Ryn Weaver"),
			new Tuple2<String, String>("Super Bass", "Nicki Minaj"),
			new Tuple2<String, String>("Defying Gravity", "Wicked"),
			new Tuple2<String, String>("Look At Me Now", "Chris Brown"),
			new Tuple2<String, String>("Boss", "Fifth Harmony"),
			new Tuple2<String, String>("Empire State Of Mind", "Jay-Z"),
			new Tuple2<String, String>("Viva La Vida", "Coldplay"),
			new Tuple2<String, String>("La Vie En Rose", "Louis Armstrong"),
			new Tuple2<String, String>("Phantom Of The Opera", "Phantom Of The Opera"),
			new Tuple2<String, String>("I Really Like You", "Carly Rae Jepsen"),
			new Tuple2<String, String>("Yesterday", "Beatles"));
}
