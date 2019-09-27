import fs from "fs";
import SparkMD5 from "spark-md5";
import { srsMap, getNextReview, repeatReview } from "./quiz";
import QParser from "q2filter";
import { shuffle, chunk, generateSecret } from "./util";
import stringify from "fast-json-stable-stringify";
import Anki, { IMedia } from "ankisync";
import { prop, Typegoose, Ref, pre, index, InstanceType } from '@hasezoey/typegoose';
import mongoose from 'mongoose';
import moment from "moment";
import { ObjectID } from "bson";
import { R2rOnline, ICondOptions, IEntry, IPagedOutput, IRender, ankiMustache, toSortedData, R2rLocal, IProgress } from "@rep2recall/r2r-format";

@pre<User>("save", async function () {
  if (!this.secret) {
    this.secret = await generateSecret();
  }
})
class User extends Typegoose {
  @prop({ required: true, unique: true }) email!: string;
  @prop() picture?: string;
  @prop({ required: true }) secret!: string;
}

const UserModel = new User().getModelForClass(User);

@index({ name: 1, user: 1 }, { unique: true })
class Deck extends Typegoose {
  @prop({ required: true }) name!: string;
  @prop({ ref: User }) user?: Ref<User>;
}

const DeckModel = new Deck().getModelForClass(Deck);

@index({ h: 1, user: 1 }, { unique: true })
class Source extends Typegoose {
  @prop({ required: true }) h!: string;
  @prop({ required: true }) name!: string;
  @prop({ required: true, default: new Date() }) created?: Date;
  @prop({ ref: User }) user?: Ref<User>;
}

const SourceModel = new Source().getModelForClass(Source);

@index({ key: 1, user: 1 }, { unique: true })
@pre<Template>("save", async function () {
  if (!this.key) {
    const { front, back, css, js } = this;
    this.key = SparkMD5.hash(stringify({ front, back, css, js }));
  }
})
class Template extends Typegoose {
  @prop({ required: true }) name!: string;
  @prop({ ref: Source }) source?: Ref<Source>;
  @prop({ required: true }) key?: string;
  @prop({ required: true }) front!: string;
  @prop() back?: string;
  @prop() css?: string;
  @prop() js?: string;
  @prop({ ref: User }) user?: Ref<User>;
}

const TemplateModel = new Template().getModelForClass(Template);

@index({ key: 1, user: 1 }, { unique: true })
@pre<Note>("save", function () {
  if (!this.key) {
    this.key = SparkMD5.hash(stringify(this.data));
  }
})
class Note extends Typegoose {
  @prop({ required: true }) key!: string;
  @prop({ required: true }) name!: string;
  @prop({ ref: Source }) source?: Ref<Source>;
  @prop({ required: true }) data!: Record<string, any>;
  @prop({ required: true }) order!: Record<string, number>;
  @prop({ ref: User }) user?: Ref<User>;
}

const NoteModel = new Note().getModelForClass(Note);

@index({ h: 1, user: 1 }, { unique: true })
@pre<Media>("save", function () {
  if (!this.h) {
    this.h = SparkMD5.ArrayBuffer.hash(this.data);
  }
})
class Media extends Typegoose {
  @prop({ required: true }) h?: string;
  @prop({ ref: Source }) source?: Ref<Source>;
  @prop({ required: true }) name!: string;
  @prop({ required: true }) data!: ArrayBuffer;
  @prop({ ref: User }) user?: Ref<User>;
}

const MediaModel = new Media().getModelForClass(Media);

@pre<Card>("save", function () {
  this.modified = new Date();
})
class Card extends Typegoose {
  @prop({ ref: Deck, required: true }) deck!: Ref<Deck>;
  @prop({ ref: Template }) template?: Ref<Template>;
  @prop({ ref: Note }) note?: Ref<Note>;
  @prop({ required: true }) front!: string;
  @prop() back?: string;
  @prop() mnemonic?: string;
  @prop() srsLevel?: number;
  @prop() nextReview?: Date;
  @prop({ default: [] }) tag?: string[];
  @prop({ default: new Date() }) created?: Date;
  @prop() modified?: Date;
  @prop() stat?: {
    streak: { right: number; wrong: number };
  };
  @prop({ ref: User }) user?: Ref<User>;
}

const CardModel = new Card().getModelForClass(Card);

export default class R2rMongo extends R2rOnline {
  public user?: InstanceType<User>;
  private mongoUri: string;

  constructor(mongoUri: string) {
    super();
    this.mongoUri = mongoUri;
  }

  public async build() {
    mongoose.set('useCreateIndex', true);
    await mongoose.connect(this.mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });
    return this;
  }

  public async signup(
    email: string,
    password: string,
    options: {picture?: string} = {}
  ): Promise<string> {
    const u = await UserModel.findOne({ email });
    if (u) {
      this.user = u;
      return u.secret;
    } else {
      const secret = await generateSecret();
      this.user = await UserModel.create({
        _id: await generateSecret(),
        email,
        secret,
        ...options
      });

      return secret;
    }
  }

  public async getSecret(): Promise<string | null> {
    return this.user ? this.user.secret : null;
  }

  public async newSecret(): Promise<string | null> {
    if (this.user) {
      const secret = await generateSecret();
      this.user.secret = secret;
      await this.user.save();
      return secret;
    }

    return null;
  }

  public async parseSecret(secret: string): Promise<boolean> {
    const u = await UserModel.findOne({ secret });
    if (u) {
      this.user = u;
      return true;
    }

    return false;
  }

  public async login(email: string, secret: string): Promise<boolean> {
    const u = await UserModel.findOne({ email, secret });
    if (u) {
      this.user = u;
      return true;
    }

    return false;
  }

  public async logout() {
    this.user = undefined;
    return true;
  }

  public async close() {
    await mongoose.disconnect();
    return this;
  }

  public async reset() {
    if (this.user) {
      await Promise.all([
        SourceModel.deleteMany({ user: this.user._id }),
        MediaModel.deleteMany({ user: this.user._id }),
        TemplateModel.deleteMany({ user: this.user._id }),
        NoteModel.deleteMany({ user: this.user._id }),
        CardModel.deleteMany({ user: this.user._id })
      ]);

      await this.user.remove();
      await this.logout();
    } else {
      await Promise.all([
        SourceModel.deleteMany({ user: { $exists: false } }),
        MediaModel.deleteMany({ user: { $exists: false } }),
        TemplateModel.deleteMany({ user: { $exists: false } }),
        NoteModel.deleteMany({ user: { $exists: false } }),
        CardModel.deleteMany({ user: { $exists: false } })
      ]);
    }

    return this;
  }

  public async parseCond(
    q: string,
    options: ICondOptions<IEntry> = {}
  ): Promise<IPagedOutput<Partial<IEntry>>> {
    if (options.sortBy === "random") {
      q += " is:random";
      delete options.sortBy;
    }

    const parser = new QParser<IEntry>(q, {
      anyOf: new Set(["template", "front", "mnemonic", "deck", "tag"]),
      isString: new Set(["template", "front", "back", "mnemonic", "deck", "tag"]),
      isDate: new Set(["created", "modified", "nextReview"]),
      transforms: {
        "is:due": () => {
          return { nextReview: { $lt: new Date() } }
        }
      },
      noParse: new Set(["is:distinct", "is:duplicate", "is:random"]),
      sortBy: {
        key: options.sortBy || "created",
        desc: options.desc || true
      }
    });

    const fullCond = parser.getCondFull();

    if (!options.fields) {
      return {
        data: [],
        count: 0
      };
    } else if (options.fields === "*") {
      options.fields = ["data", "source", "deck", "front", "js", "mnemonic", "modified",
        "nextReview", "sCreated", "sH", "srsLevel", "stat", "tBack", "tFront", "tag",
        "template", "back", "created", "css", "_id"];
    }

    const allFields = new Set<string>(options.fields || []);
    for (const f of (fullCond.fields || [])) {
      allFields.add(f);
    }

    if (q.includes("is:distinct") || q.includes("is:duplicate")) {
      allFields.add("key");
    }

    const proj = {} as { [k: string]: 1 | 0 };

    if (["data", "order"].some((k) => allFields.has(k))) {
      proj.note = 1;
    }

    if (["deck"].some((k) => allFields.has(k))) {
      proj.deck = 1;
    }

    if (["sCreated", "sH", "source"].some((k) => allFields.has(k))) {
      proj.source = 1;
    }

    if (["tFront", "tBack", "template", "model", "css", "js"].some((k) => allFields.has(k))) {
      proj.template = 1;
    }

    for (const f of allFields) {
      proj[f] = 1;
    }

    const outputProj = { id: 1 } as { [k: string]: 1 | 0 };

    for (const f of options.fields) {
      proj[f] = 1;
      outputProj[f] = 1;
    }

    let aggArray: any[] = [
      { $match: { user: this.user!._id } },
      { $project: proj }
    ];

    if (["data", "order", "key"].some((k) => allFields.has(k))) {
      aggArray.push(
        {
          $lookup: {
            from: "note",
            localField: "note",
            foreignField: "_id",
            as: "n"
          }
        },
        {
          $unwind: {
            path: "$n",
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            ...outputProj,
            data: "$n.data",
            order: "$n.order",
            key: "$n.key"
          }
        }
      );
    }

    if (["deck"].some((k) => allFields.has(k))) {
      aggArray.push(
        {
          $lookup: {
            from: "deck",
            localField: "deck",
            foreignField: "_id",
            as: "d"
          }
        },
        {
          $unwind: {
            path: "$d",
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            ...outputProj,
            deck: "$d.name"
          }
        }
      );
    }

    if (["sCreated", "sH", "source"].some((k) => allFields.has(k))) {
      aggArray.push(
        {
          $lookup: {
            from: "source",
            localField: "source",
            foreignField: "_id",
            as: "s"
          }
        },
        {
          $unwind: {
            path: "$s",
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            ...outputProj,
            source: "$s.name",
            sH: "$s.h",
            sCreated: "$s.created"
          }
        }
      );
    }

    if (["tFront", "tBack", "template", "css", "js"].some((k) => allFields.has(k))) {
      aggArray.push(
        {
          $lookup: {
            from: "template",
            localField: "template",
            foreignField: "_id",
            as: "t"
          }
        },
        {
          $unwind: {
            path: "$t",
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $project: {
            ...outputProj,
            tFront: "$t.front",
            tBack: "$t.back",
            template: "$t.name",
            css: "$t.css",
            js: "$t.js"
          }
        }
      );
    }

    aggArray.push({ $match: fullCond.cond });

    const getGroupStmt = (k0: string) => {
      return {
        $group: {
          _id: `$${k0}`, repeat: { $sum: 1 }, data: {
            $addToSet: (() => {
              const newProj = {} as any;

              for (const k of Object.keys(outputProj)) {
                newProj[k] = `$${k}`;
              }

              return newProj;
            })()
          }
        }
      };
    }
    const projStmt = (() => {
      const newProj = {} as any;

      for (const k of Object.keys(outputProj)) {
        newProj[k] = `$data.${k}`;
      }

      return newProj;
    })();

    if (fullCond.noParse.has("is:distinct")) {
      aggArray.push(
        getGroupStmt("key"),
        { $project: { data: { $arrayElemAt: ["$data", 0] } } },
        { $project: projStmt }
      )
    }

    if (fullCond.noParse.has("is:duplicate")) {
      aggArray.push(
        getGroupStmt("front"),
        { $match: { repeat: { $gt: 1 } } },
        { $unwind: "$data" },
        { $project: projStmt }
      )
    }

    const ids = await CardModel.aggregate([
      ...aggArray,
      {
        $project: {
          _id: 1
        }
      }
    ]);

    aggArray.push(
      { $project: outputProj }
    );

    if (fullCond.noParse.has("is:random")) {
      shuffle(ids);
      aggArray = [
        { $match: { _id: { $in: ids.slice(0, options.limit) } } },
        ...aggArray
      ]
    } else {
      const sortBy = fullCond.sortBy ? fullCond.sortBy.key : options.sortBy;
      const desc = fullCond.sortBy ? fullCond.sortBy.desc : options.desc;
      if (sortBy) {
        aggArray.push(
          { $sort: { [sortBy]: desc ? -1 : 1 } }
        )
      }

      aggArray.push(
        { $skip: options.offset },
        { $limit: options.limit }
      )
    }

    const data = await CardModel.aggregate(aggArray);

    return {
      data,
      count: ids.length
    };
  }

  public async insertMany(entries: IEntry[]): Promise<string[]> {
    entries = await entries.mapAsync(async (e) => await this.transformCreateOrUpdate(null, e)) as IEntry[];
    const now = new Date();

    const sIdMap: Record<string, ObjectID> = {};

    await entries.filter((e) => e.sH).distinctBy((el) => el.sH!).mapAsync(async (el) => {
      try {
        sIdMap[el.sH!] = (await SourceModel.create({
          name: el.source!,
          created: el.sCreated ? moment(el.sCreated).toDate() : now,
          h: el.sH!
        }))._id
      } catch (err) { }
    });

    const tIdMap: Record<string, ObjectID> = {};

    await entries.filter((e) => e.tFront).mapAsync(async (el) => {
      try {
        tIdMap[el.template!] = (await TemplateModel.create({
          name: el.template!,
          front: el.tFront!,
          back: el.tBack,
          css: el.css,
          js: el.js,
          sourceId: el.sH ? sIdMap[el.sH] : undefined
        }))._id;
      } catch (e) { }
    });

    const nDataMap: Record<string, string> = {};
    const ns = entries.filter((e) => e.data).map((el) => {
      const data: Record<string, any> = {};
      const order: Record<string, number> = {};

      let index = 1;
      for (const { key, value } of el.data!) {
        data[key] = value;
        order[key] = index
        index++;
      }

      const key = SparkMD5.hash(stringify(data));

      nDataMap[SparkMD5.hash(stringify(el.data!))] = key;

      return {
        key,
        name: `${el.sH}/${el.template}/${el.data![0].value}`,
        data,
        order,
        sourceId: el.sH ? sIdMap[el.sH] : undefined
      }
    });

    const nIdMap: Record<string, ObjectID> = {};

    try {
      await NoteModel.insertMany(ns, { ordered: false }, (err, docs) => {
        for (const el of entries) {
          for (const d of docs) {
            if (nDataMap[SparkMD5.hash(stringify(el.data!))] === d.key) {
              (el as any).noteId = d._id;
              break;
            }
          }
        }
      });
    } catch (e) { }

    const dMap: { [key: string]: ObjectID } = {};
    const decks = entries.map((e) => e.deck);
    const deckIds = await decks.mapAsync((d) => this.getOrCreateDeck(d));
    decks.forEach((d, i) => {
      dMap[d] = new ObjectID(deckIds[i]);
    });

    return (await CardModel.insertMany(entries.map((e) => {
      return {
        front: e.front,
        back: e.back,
        mnemonic: e.mnemonic,
        srsLevel: e.srsLevel,
        nextReview: e.nextReview,
        deckId: dMap[e.deck],
        noteId: (e as any).noteId,
        templateId: e.template ? tIdMap[e.template] : undefined,
        created: now,
        tag: e.tag
      }
    }))).map((el) => el._id.toString());
  }

  public async updateMany(ids: string[], u: Partial<IEntry>) {
    const cs = await (await CardModel.find({ _id: { $in: ids.map((id) => new ObjectID(id)) } })).mapAsync(async (c) => {
      for (const k of Object.keys(c)) {
        if (!Object.keys(u).includes(k) && k !== "_id") {
          delete (c as any)[k];
        }
      }

      const c0: any = Object.assign(c, await this.transformCreateOrUpdate(c._id!, u));
      const c1: any = { _id: c._id! };

      for (let [k, v] of Object.entries(c0)) {
        switch (k) {
          case "deck":
            k = "deckId",
              v = await this.getOrCreateDeck(v as string);
            c1[k] = v;
            break;
          case "tFront":
          case "tBack":
            k = k.substr(1).toLocaleLowerCase();
          case "css":
          case "js":
            const { template } = (await CardModel.findOne({ _id: c._id! }))!;
            await TemplateModel.updateOne({ _id: template }, {
              $set: { [k]: v }
            });
            break;
          case "data":
            const { note } = (await CardModel.findOne({ _id: c._id! }).populate("note"))!;
            console.log(note);
            if (note) {
              const { order, data } = note as Note;
              for (const { key, value } of v as any[]) {
                if (!order![key]) {
                  order![key] = Math.max(...Object.values(order!)) + 1;
                }
                data![key] = value;
              }
              await NoteModel.updateOne({ _id: (note as any)._id }, {
                $set: { order, data }
              });
            } else {
              const order: Record<string, number> = {};
              const data: Record<string, any> = {};
              for (const { key, value } of v as any[]) {
                if (!order[key]) {
                  order[key] = Math.max(-1, ...Object.values(order)) + 1;
                }
                data[key] = value;
              }

              const key = SparkMD5.hash(stringify(data))
              const name = `${key}/${Object.values(data)[0]}`;
              c1.note = await NoteModel.create({ key, name, order, data });
            }
            break;
          default:
            c1[k] = v;
        }
      }

      return c1;
    });

    await cs.mapAsync(async (c) => {
      if (Object.keys(c).length > 1) {
        const { _id, ...$set } = c;
        await CardModel.updateOne({ _id }, { $set });
      }
    });
  }

  public async addTags(ids: string[], tags: string[]) {
    await CardModel.updateMany({ _id: { $in: ids.map((id) => new ObjectID(id)) } }, {
      $addToSet: { tag: { $each: tags } }
    });
  }

  public async removeTags(ids: string[], tags: string[]) {
    await CardModel.updateMany({ _id: { $in: ids.map((id) => new ObjectID(id)) } }, {
      $pull: { tag: { $in: tags } }
    });
  }

  public async deleteMany(ids: string[]) {
    await CardModel.deleteMany({ _id: { $in: ids.map((id) => new ObjectID(id)) } });
  }

  public async render(cardId: string) {
    const r = await this.parseCond(`_id=${cardId}`, {
      limit: 1,
      fields: ["front", "back", "mnemonic", "tFront", "tBack", "data", "css", "js"]
    });

    const c = r.data[0] as IRender;
    const { tFront, tBack, data } = c;

    if (/@md5\n/.test(c.front)) {
      c.front = ankiMustache(tFront || "", data);
    }

    if (c.back && /@md5\n/.test(c.back)) {
      c.back = ankiMustache(tBack || "", data, c.front);
    }

    return c;
  }

  protected async updateSrsLevel(dSrsLevel: number, cardId: string) {
    const card = await CardModel.findOne({ _id: new ObjectID(cardId) });

    if (!card) {
      return;
    }

    card.srsLevel = card.srsLevel || 0;
    card.stat = card.stat || {
      streak: {
        right: 0,
        wrong: 0
      }
    };
    card.stat.streak = card.stat.streak || {
      right: 0,
      wrong: 0
    }

    if (dSrsLevel > 0) {
      card.stat.streak.right = (card.stat.streak.right || 0) + 1;
    } else if (dSrsLevel < 0) {
      card.stat.streak.wrong = (card.stat.streak.wrong || 0) + 1;
    }

    card.srsLevel += dSrsLevel;

    if (card.srsLevel >= srsMap.length) {
      card.srsLevel = srsMap.length - 1;
    }

    if (card.srsLevel < 0) {
      card.srsLevel = 0;
    }

    if (dSrsLevel > 0) {
      card.nextReview = getNextReview(card.srsLevel);
    } else {
      card.nextReview = repeatReview();
    }

    const { srsLevel, stat, nextReview } = card;
    this.updateMany([cardId], { srsLevel, stat, nextReview });
  }

  protected async transformCreateOrUpdate(
    cardId: string | null,
    u: Partial<IEntry>
  ): Promise<Partial<IEntry>> {
    let data: Record<string, any> | null = null;
    let front: string = "";

    if (u.front && u.front.startsWith("@template\n")) {
      if (!data) {
        if (cardId) {
          data = this.getData(cardId);
        } else {
          data = u.data || [];
        }
      }

      u.tFront = u.front.substr("@template\n".length);
    }

    if (u.tFront) {
      front = ankiMustache(u.tFront, data || {});
      u.front = "@md5\n" + SparkMD5.hash(front);
    }

    if (u.back && u.back.startsWith("@template\n")) {
      if (!data) {
        if (cardId) {
          data = this.getData(cardId);
        } else {
          data = u.data || [];
        }
      }

      u.tBack = (u.front || "").substr("@template\n".length);
      if (!front && cardId) {
        front = await this.getFront(cardId);
      }
    }

    if (u.tBack) {
      const back = ankiMustache(u.tBack, data || {}, front);
      u.back = "@md5\n" + SparkMD5.hash(back);
    }

    return u;
  }

  protected async getOrCreateDeck(name: string): Promise<string> {
    try {
      return (await DeckModel.create({ name }))._id.toString();
    } catch (e) {
      return (await DeckModel.findOne({ name }))!._id.toString();
    }
  }

  protected async getData(cardId: string): Promise<Array<{ key: string, value: any }> | null> {
    const c = await CardModel.findOne({ _id: new ObjectID(cardId) }).populate("note", "data");
    if (c && c.note) {
      const { data, order } = c.note as Note;
      return toSortedData({ data, order });
    }

    return null;
  }

  protected async getFront(cardId: string): Promise<string> {
    const c = await CardModel.findOne({ _id: new ObjectID(cardId) }).populate("note", "data").populate("template", "front");
    if (c && c.note && c.template) {
      if (c.front.startsWith("@md5\n")) {
        return ankiMustache((c.template as Template).front, (c.note as Note).data || {});
      }

      return c.front;
    }

    return "";
  }

  public async export(r2r: R2rLocal, q: string = "",
    options?: { callback?: (p: IProgress) => void }
  ) {
    const callback = options ? options.callback : undefined;
    let current = 1;

    const ms = await MediaModel.find();
    for (const m of ms) {
      if (callback) callback({ text: "Inserting media", current, max: ms.length });
      try {
        await r2r.createMedia(m as IMedia);
      } catch (e) { }
      current++;
    }

    if (callback) callback({ text: "Parsing q" })
    const cs = await this.parseCond(q, {
      fields: "*"
    });

    current = 1;
    for (const c of chunk(cs.data as IEntry[], 1000)) {
      if (callback) callback({ text: "Inserting cards", current, max: cs.count });
      await r2r.insertMany(c);
      current += 1000;
    }

    await r2r.close();
  }

  public async getMedia(h: string): Promise<IMedia | null> {
    const m = await MediaModel.findOne({ h }) as IMedia;
    return m || null;
  }

  public async allMedia() {
    return (await MediaModel.find({})) as IMedia[];
  }

  public async createMedia(m: { name: string, data: ArrayBuffer }) {
    const h = SparkMD5.ArrayBuffer.hash(m.data);
    await MediaModel.create({ ...m, h });
    return h;
  }

  public async deleteMedia(h: string) {
    await MediaModel.deleteOne({ h });
    return true;
  }

  public async fromR2r(r2r: R2rLocal, options?: { filename?: string, callback?: (p: IProgress) => void }) {
    const filename = options ? options.filename : undefined;
    const callback = options ? options.callback : undefined;

    if (callback) callback({ text: "Reading R2r file" });

    const data = fs.readFileSync(r2r.filename);
    const sourceH = SparkMD5.ArrayBuffer.hash(data);
    const now = new Date();
    let sourceId: ObjectID;

    try {
      sourceId = (await SourceModel.create({
        name: filename || r2r.filename,
        h: sourceH,
        created: now
      }))._id;
    } catch (e) {
      if (callback) callback({ text: "Duplicated Anki resource" });
      return;
    }

    await (await r2r.allMedia()).mapAsync(async (m) => {
      try {
        MediaModel.create({
          ...m,
          sourceId
        });
      } catch (e) { }
    });

    await this.insertMany((await r2r.parseCond("", {
      fields: "*"
    })).data as IEntry[]);
  }

  public async fromAnki(
    anki: Anki,
    options?: {
      filename?: string, callback?: (p: {
        text: string;
        current?: number;
        max?: number;
      }) => void
    }
  ) {
    const filename = options ? options.filename : undefined;
    const callback = options ? options.callback : undefined;

    if (callback) callback({ text: "Reading Anki file" });

    const data = fs.readFileSync(anki.filePath);
    let sourceId: ObjectID;
    const sourceH = SparkMD5.ArrayBuffer.hash(data);
    const now = new Date();

    try {
      sourceId = (await SourceModel.create({
        name: filename || anki.filePath,
        h: SparkMD5.ArrayBuffer.hash(data),
        created: now
      }))._id;
    } catch (e) {
      if (callback) callback({ text: "Duplicated Anki resource" });
      return;
    }

    let current: number;
    let max: number;

    const media = await anki.apkg.tables.media.all();
    current = 0;
    max = media.length;
    for (const el of media) {
      if (callback) callback({ text: "Inserting media", current, max });

      try {
        await MediaModel.create({
          h: el.h,
          name: el.name,
          data: el.data,
          sourceId
        })
      } catch (e) { }

      current++;
    }

    const card = await anki.apkg.tables.cards.all();
    const dIdMap: Record<string, ObjectID> = {};
    const tIdMap: Record<string, ObjectID> = {};
    const nIdMap: Record<string, ObjectID> = {};

    current = 0;
    max = card.length;

    for (const cs of chunk(card, 1000)) {
      if (callback) callback({ text: "Inserting cards", current, max });

      const decks: any[] = [];
      for (const c of cs) {
        if (!dIdMap[c.deck.name]) {
          decks.push({
            name: c.deck.name,
            user: this.user
          });
          dIdMap[c.deck.name] = new ObjectID();
        }
      }

      if (decks.length > 0) {
        await decks.mapAsync(async (d) => {
          try {
            dIdMap[d.name] = (await DeckModel.create(d))._id;
          } catch (e) {
            dIdMap[d.name] = (await DeckModel.findOne(d))!._id;
          }
        });
      }

      const templates: any[] = [];
      for (const c of cs) {
        const key = SparkMD5.hash(stringify({
          front: c.template.qfmt,
          back: c.template.afmt,
          css: c.note.model.css
        }));
        if (!tIdMap[key]) {
          templates.push({
            name: `${sourceH}/${c.note.model.name}/${c.template.name}`,
            sourceId: sourceH,
            front: c.template.qfmt,
            back: c.template.afmt,
            css: c.note.model.css,
            key,
            user: this.user
          });
          tIdMap[key] = new ObjectID();
        }
      }

      if (templates.length > 0) {
        await TemplateModel.insertMany(templates, { ordered: false }, (err, docs) => {
          for (const d of docs) {
            tIdMap[d.key!] = d._id;
          }
        })
      }

      const notes: any[] = [];
      for (const c of cs) {
        const data: Record<string, string> = {};
        const order: Record<string, number> = {};
        c.template.model.flds.forEach((k, i) => {
          data[k] = c.note.flds[i];
          order[k] = i;
        });
        const key = SparkMD5.hash(stringify(data));
        if (!nIdMap[key]) {
          notes.push({
            key,
            name: `${sourceH}/${c.note.model.name}/${c.template.name}/${c.note.flds[0]}`,
            data,
            order
          });
          nIdMap[key] = new ObjectID();
        }
      }

      if (notes.length > 0) {
        await NoteModel.insertMany(notes, { ordered: false }, (err, docs) => {
          for (const d of docs) {
            nIdMap[d.key] = d._id;
          }
        })
      }

      try {
        await CardModel.insertMany(cs.map((c) => {
          const data: Record<string, string> = {};
          c.template.model.flds.forEach((k, i) => {
            data[k] = c.note.flds[i];
          });
          const key = SparkMD5.hash(stringify(data));

          const front = ankiMustache(c.template.qfmt, data);
          const back = ankiMustache(c.template.afmt, data, front);

          return {
            deckId: dIdMap[c.deck.name],
            templateId: tIdMap[SparkMD5.hash(stringify({
              front: c.template.qfmt,
              back: c.template.afmt,
              css: c.note.model.css
            }))],
            noteId: nIdMap[key],
            front: `@md5\n${SparkMD5.hash(front)}`,
            back: `@md5\n${SparkMD5.hash(back)}`,
            tag: c.note.tags,
            created: now
          }
        }), { ordered: false });
      } catch (e) { }

      current += 1000;
    }
  }
}