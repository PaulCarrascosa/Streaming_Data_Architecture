import base64
import pytesseract
from app.pyspark_ocr import tesseract_with_spark
 
 
def test_tesseract_with_spark():
    list_images = []
    with open("./tests/ressources/test.png", "rb") as image_file:
        sda2 = base64.b64encode(image_file.read()).decode('utf-8')
        list_images.append(sda2)
    with open("./tests/ressources/Capture d’écran 2026-03-04 111850.png", "rb") as image_file:
        sda3 = base64.b64encode(image_file.read()).decode('utf-8')
        list_images.append(sda3)
    result = tesseract_with_spark(list_images)
    assert len(result) == 2
    assert result[0]["extracted_text"] == 'TD3 : Streaming avec Bytewax\n\nTest unitaire\n\ne Créer un répertoire tests/ avec un fichier test_main.py\ne uv add pytest kafka-python --group dev\ne Générer le test:\n\nwrite pytest in order to produce an image in base64 in Kafka topic \'\n\npython\n\ne Exemple de connexion depuis le test unitaire :\n@pytest.fixture\ndef kafka_producer():\nproducer = KafkaProducer(bootstrap_servers=\'localhost:9092\')\nyield producer\nproducer.close()\n\ne Lancer le test\n\ne Lancer le flow Bytewax\n\ne Vérifier que le message produit dans “in-topic” est bien transfé\ndans le topic “out-topic”\n\n \n\n‘in-topic" using library kafka-\n\no«¢ o on localhost:\n} urtorapacheatka 8355260\nDashboard Topics / out-topic\nlocal - A\n\n5 Overview Messages Consumers _— Settings __ Statistics\nrokers\n\n \n\nSeek T Partitions\nTopics ype\n\nOffset ~ | Offset Allitems are selectec\nConsumers\n\nQ + Add Filters\n\nTimestamp\n17/12/2025 17:03:14\n17/12/2025 17:03:14\n\n17/12/2025 17:03:14\n\nGaaa\n\n17/12/2025 17:03:14\n\nmB wo NN = 00\noo fo © og\n\na\n\n17/12/2025 17:04:03\n\x0c'
    assert result[1]["extracted_text"] == 'TD4 : Cluster Spark\nDeéclencher job PySpark depuis un test\n\ne Dans le projet bytewax_consumer :\ne uv add numpy pandas pyspark==4.0.1 pytesseract\ne Créer un fichier pyspark_ocr.py et avec le prompt:\nwrite with pyspark DataFrame and udf a job to submit list of images to tesseract OCR\ne Attention la fonction qui est wrappée par udf doit se trouver a Vintérieur de la fonction qui\n\ndéclenche le job, exemple :\ndef perform_ocr(image_path):\nimage = Image.open(image_path)\ntext = pytesseract.image_to_string(image)\nreturn text\nocr_udf = udf(perform_ocr, StringType())\ne Ecrire un test unitaire test_pyspark_ocr.py avec le prompt:\nwrite a pytest for method tesseract_with_spark by encoding a list of images in base64\n\ne Lancer le test unitaire en mode debug\n\x0c'