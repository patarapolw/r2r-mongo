import R2r from "../src";
import Anki from "ankisync";
import dotenv from "dotenv";
dotenv.config({
  path: "../.env"
});

(async () => {
  const r2r = await R2r.connect(process.env.MONGO_URI!);
  await r2r.reset();
  const anki = await Anki.connect("/Users/patarapolw/Downloads/Hanyu_Shuiping_Kaoshi_HSK_all_5000_words_high_quality.apkg");
  await r2r.fromAnki(anki, {callback: console.log});
  await anki.close();
  await r2r.close();
})().catch(console.error);
